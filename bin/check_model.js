// 执行为 run，本工具用于检查，没有回滚功能MAIN
if (typeof MODE == "undefined" || MODE == null || MODE == "") {
    var MODE = "test";
}

// 保留多少位小数
var FIXNUM = 4;
var CONFIGJSON = "conf/config.json";
var MODELDIR = "conf/model/";
var CLCSV = "output/cl_dimension.csv";
var CLTYPECSV = "output/cltype_dimension.csv";
var CURRENTMODELJSON = "output/current_model.json";
var ADDCLCSV = "conf/add_cl.csv";

// 日期长度，用于把切分键的时间字符串转换为时间，默认兼容 20240101 和 20240101000000 两种格式
var DATEFORMAT = { year: 4, month: 6, day: 8, hour: 10, minute: 12, second: 14};
// 普通表默认时间间隔，用于仅有一张普通 LOB 表，无法自动推算出间隔时；如果是分区表，会从上下界推算，不使用该值
var DEFAULTTIMEINTERVALOBJ = {"diffYear": 1, "diffMonth": 0, "diffDay": 0};

var TOOL = "check_model";
// 主表:应用名
var MAINSUBCLAPPNAMEOBJ = {};
// 普通表:应用名
var NORMALCLAPPNAMEOBJ = {};
// 子表:主表
var SUBMAINOBJ = {};
// 主表:最后一个子表
var LASTSUBOBJ = {};
// 普通表:最后一个时间命名表
var LASTNORMALOBJ = {};
// 主表:MAX表
var MAINMAX = {};
// 主（普通）表:表类型
var CLTYPE = {};
// 主表:切分键字符串
var MAINCLKEY = {};
// 主表:切分键OBJ
var MAINCLKEYOBJ = {};
// 普通表:切分键OBJ
var NORMALCLKEYOBJ = {};
// 主表:扩展分区下界
var EXTENDLOW = {};
// 主表:扩展分区上界
var EXTENDUP = {};
// 主表:扩展表名数组
var EXTENDNAME = {};
// 主表:扩展上下界
var EXTENDBOUND = {};
// 表:查找条件
var CLFINDCOND = {};
// 最后一张子表表名: 时间字符串
var EXTENDNAMEDATESTR = {};
// 最后一张子表表名: 扩展表数量
var EXTENDCLCOUNT = {};

var INCLUDEMAXRANGE = false;
importOnce("./args.js");
importOnce("./general.js");

function changDate(str) {
    if (str.search("\\$DATE") == -1) {
        return [str];
    }

    let retArray = [];
    for (let i = 0; i < DATE_FORMAT_ARRAY.length; i++) {
        let dateStr = DATE_FORMAT_ARRAY[i].replace(new RegExp("\\$YYYY", "i"), "[1-2]\\d{3}");
        dateStr = dateStr.replace("$MM", "[0-1][0-9]");
        dateStr = dateStr.replace(new RegExp("\\$dd", "i"), "[0-3][0-9]");
        // dateStr = dateStr.replace(new RegExp("HH", "i"), "[0-2][0-9]");
        // dateStr = dateStr.replace("mm", "[0-5][0-9]");
        // dateStr = dateStr.replace(new RegExp("ss", "i"), "[0-5][0-9]");
        retArray.push(str.replace(new RegExp("\\$DATE", "g"), dateStr));
    }
    return retArray;
}

function findCL(CLNameArray, type) {
    let CLArray = [];
    for (let i = 0; i < CLNameArray.length; i++) {
        let CLNameObj = CLNameArray[i];
        let keys = Object.keys(CLNameObj);
        let CLtype = keys[0];
        let nameArray = CLNameObj[CLtype].split('.');
        if (nameArray.length != 2) {
            let content = "bin/args.js 文件中 [" + JSON.stringify(CLNameObj) + "] 格式有问题，请检查";
            logger.error(content);
            throw new Error(content);
        }
        let cs = nameArray[0];
        let cl = nameArray[1];
        let csArray = cs.split('$APPNAME');
        let clArray = cl.split('$APPNAME');
        if (csArray.length > 2 || clArray.length > 2 ) {
            let content = "bin/args.js 文件中 [" + JSON.stringify(CLNameObj) + "] 格式有问题，目前 CS 或 CL 中只能有一个 $APPNAME";
            logger.error(content);
            throw new Error(content);
        }
        let cs_prefix = csArray[0];
        let cs_suffix = csArray[1];
        let cl_prefix = clArray[0];
        let cl_suffix = clArray[1];
        let condArray = changDate("^" + cs_prefix + "([^\.]*)" + cs_suffix + "\." + cl_prefix + "\\1" + cl_suffix + "$");
        for (let j = 0; j < condArray.length; j++) {
            try {
                let cursor = db.list(4,{Name: {"$regex": condArray[j]}},{Name:1});
                let CLname = "";
                while (cursor.next()) {
                    CLname = cursor.current().toObj()['Name'];
                    CLArray.push(CLname);
                    // 记录表与应用名关系，无序
                    switch (type) {
                        case "MainSubCL":
                            MAINSUBCLAPPNAMEOBJ[CLname] = CLname.replace(new RegExp(condArray[j]),"$1");
                            CLTYPE[CLname] = CLtype;
                            break;
                        case "NormalCL":
                            NORMALCLAPPNAMEOBJ[CLname] = CLname.replace(new RegExp(condArray[j]),"$1");
                            CLTYPE[CLname] = CLtype;
                            break;
                        default:
                            break;
                    }
                    CLFINDCOND[CLname] = CLNameObj[CLtype];
                }
                // 通过排序，找出同类型普通表中最后一张时间表
                if ( "NormalCL" == type && "" != CLname) {
                    CLArray.sort();
                    let lastCL = CLArray[0];
                    let lastApp = NORMALCLAPPNAMEOBJ[lastCL];
                    let l = 0;
                    for (let k = 1; k < CLArray.length; k++){
                        if (lastApp != NORMALCLAPPNAMEOBJ[CLArray[k]]) {
                            for (; l < k; l++) {
                                LASTNORMALOBJ[CLArray[l]] = lastCL;
                            }
                            lastApp = NORMALCLAPPNAMEOBJ[CLArray[k]];
                            l = k;
                        } else {
                            lastCL = CLArray[k];
                        }
                    }
                }
            } catch (error) {
                logger.except("Failed to find the CL with [" +  JSON.stringify(CLNameObj) + "]", error);
                throw error;
            }
        }
    }
    return CLArray;
}

// to YYYYMMddHHmmss
function date2Str(date) {
    let dateStr = '' + date.getFullYear();
    if (date.getMonth() < 9) {
        dateStr += ('0' + (date.getMonth() + 1));
    } else {
        dateStr += (date.getMonth() + 1);
    }

    if (date.getDate() < 10) {
        dateStr += ('0' + date.getDate());
    } else {
        dateStr += date.getDate();
    }

    return dateStr;
}

function parseDateTime(timeStr) {
    let year = parseInt(timeStr.substring(0, DATEFORMAT.year), 10);
    // 月份从 0 开始算，需要 -1
    let month = 0;
    let day = 1;
    if (timeStr.length == DATEFORMAT.month) {
        let month = parseInt(timeStr.substring(DATEFORMAT.year, DATEFORMAT.month), 10) - 1;
    }
    if (timeStr.length == DATEFORMAT.month) {
        day = parseInt(timeStr.substring(DATEFORMAT.month, DATEFORMAT.day), 10);
    }

    if (timeStr.length == DATEFORMAT.second) {
        let hour = parseInt(timeStr.substring(DATEFORMAT.day, DATEFORMAT.hour), 10);
        let monute = parseInt(timeStr.substring(DATEFORMAT.hour, DATEFORMAT.minute), 10);
        let second = parseInt(timeStr.substring(DATEFORMAT.minute, DATEFORMAT.second), 10);
        if (hour != 0 || monute != 0 || second != 0) {
            let content = "目前不支持主表 range 切分键时分秒不为 0";
            logger.error(content);
            throw new Error(content);
        }
        return new Date(year, month, day, hour, monute, second);
    } else {
        return new Date(year, month, day);
    }
}

function getTimeIntervalObj(lowTimeStr, upTimeStr) {
    try {
        const lowDateDate = parseDateTime(lowTimeStr);
        const upDateDate = parseDateTime(upTimeStr);
        //logger.info(lowDateDate + " " + upDateDate);
        const diffYear = Math.abs(upDateDate.getFullYear() - lowDateDate.getFullYear());
        const diffMonth = Math.abs(upDateDate.getMonth() - lowDateDate.getMonth());
        const diffDay = Math.abs(upDateDate.getDate() - lowDateDate.getDate());
        
        return {"diffYear": diffYear, "diffMonth": diffMonth, "diffDay": diffDay};
    } catch (error) {
        logger.except("Failed to get TimeInterval", error);
        throw error;
    }
}

function addTimeIntervalObj(date, timeIntervalObj) {
    let retDate = new Date(date);
    if (timeIntervalObj.diffDay) {
        retDate.setDate(retDate.getDate() + timeIntervalObj.diffDay);
    } else if (timeIntervalObj.diffMonth) {
        retDate.setMonth(retDate.getMonth() + timeIntervalObj.diffMonth);
    } else if (timeIntervalObj.diffYear) {
        retDate.setFullYear(retDate.getFullYear() + timeIntervalObj.diffYear);
    } else {
        let content = "无法生成新分区时间，时间差为: " + JSON.stringify(timeIntervalObj);
        logger.error(content);
        throw new Error(content);
    }

    return retDate;
}

// 记录原始时间字符串
// 按照格式生产一些列时间字符串
// 替换
function getExtendCLNameArray(lastSubCLName, lastUpBound, timeIntervalObj) {
    let endTimeDate = parseDateTime(ENDTIME);
    let lastUpBoundDate = parseDateTime(lastUpBound);
    const curDate = new Date();
    if (curDate.getTime() > endTimeDate.getTime()) {
        let content = "conf/args.js 中 ENDTIME 在当前时间之前，请修改";
        logger.error(content);
        throw new Error(content);
    }
    let extendCLNameArray = [];

    // 根据时间格式字符串查找，逻辑是最后一个分区（除 MAX）有当前时间的年、月、日（由最小精度决定）
    for (let i = 0; i < DATE_FORMAT_ARRAY.length; i++){
        let dateFormat = DATE_FORMAT_ARRAY[i];
        let csName = lastSubCLName.split('.')[0];
        let clName = lastSubCLName.split('.')[1];

        // 通过分区时间差倒推名字，如果分区时间差一年，那么只关心 20240101 中的 2024 ，其余不关心，但目前无法处理 240101 这种的（应该不会有客户用，有歧义）
        // 考虑提前建了后面分区的情况，即传入的子表名为 2025，但当前为 2024
        let newDate = new Date(curDate);
        let CLCount = 0;
        // 根据 EXTENDCLCOUNT 来做循环条件
        while (newDate.getTime() < endTimeDate.getTime() || newDate.getTime() < lastUpBoundDate.getTime()) {
            let oldDateStr = dateFormat.replace(new RegExp("\\$YYYY", "i"), newDate.getFullYear());

            // 获取现有表名中需要替换的老字符串
            let month = "";
            let day = "";
            // 补齐月份和日期的0
            if (newDate.getMonth() < 9) {
                month += ('0' + (newDate.getMonth() + 1));
            } else {
                month += (newDate.getMonth() + 1);
            }
            if (newDate.getDate() < 10) {
                day += ('0' + newDate.getDate());
            } else {
                day += newDate.getDate();
            }
            oldDateStr = oldDateStr.replace("$MM", month);
            oldDateStr = oldDateStr.replace(new RegExp("\\$dd", "i"), day);

            // 更新日期
            newDate = addTimeIntervalObj(newDate, timeIntervalObj);
            // 替换新字符串中时间
            let newDateStr = dateFormat.replace(new RegExp("\\$YYYY", "i"), newDate.getFullYear());
            // 补齐月份和日期的0
            month = "";
            day = "";
            if (newDate.getMonth() < 9) {
                month += ('0' + (newDate.getMonth() + 1));
            } else {
                month += (newDate.getMonth() + 1);
            }
            if (newDate.getDate() < 10) {
                day += ('0' + newDate.getDate());
            } else {
                day += newDate.getDate();
            }
            newDateStr = newDateStr.replace("$MM", month);
            newDateStr = newDateStr.replace(new RegExp("\\$dd", "i"), day);
            //logger.info(oldDateStr + " " + newDateStr);
            if (-1 != csName.search(oldDateStr) || -1 != clName.search(oldDateStr)) {
                csName = csName.replace(oldDateStr, newDateStr);
                clName = clName.replace(oldDateStr, newDateStr);
                //logger.info(EXTENDCLCOUNT[lastSubCLName]);
                if (CLCount < EXTENDCLCOUNT[lastSubCLName] || lastUpBound == "") {
                    extendCLNameArray.push(csName + '.' + clName);
                }

                //logger.info("扩展子表名: " + csName + '.' + clName + " " + CLCount);
                // 获取模型中时间格式
                let modelDateFormat = "$YYYY";
                if (timeIntervalObj.month) {
                    modelDateFormat += "$MM";
                }
                if (timeIntervalObj.day) {
                    modelDateFormat += "$dd";
                }
                //logger.info(lastSubCLName + " " + oldDateStr + " " + modelDateFormat);
                if (EXTENDNAMEDATESTR[lastSubCLName] == undefined) {
                    EXTENDNAMEDATESTR[lastSubCLName] = { "dateStr": oldDateStr, "dateFormat": modelDateFormat};
                }
                CLCount++;
            }
        }
    }
    return extendCLNameArray;
}

function getextendCLRangeArray(lastSubCLName, firstLowBoundStr, timeIntervalObj) {
    let lowBoundDate = parseDateTime(firstLowBoundStr);
    let endTimeDate = parseDateTime(ENDTIME);
    const curDate = new Date();
    if (curDate.getTime() > endTimeDate.getTime()) {
        let content = "conf/args.js 中 ENDTIME 在当前时间之前，请修改";
        logger.error(content);
        throw new Error(content);
    }

    let extendCLRangeArray = [];
    let lowBoundStr = firstLowBoundStr;
    let upBoundStr = date2Str(addTimeIntervalObj(lowBoundDate, timeIntervalObj));
    let loop = 0;
    //logger.info(lowBoundDate + " " + endTimeDate);
    while (lowBoundDate.getTime() < endTimeDate.getTime()) {
        //logger.info("loop " + lowBoundDate + " " + endTimeDate);
        extendCLRangeArray.push({"LowBound":lowBoundStr, "UpBound":upBoundStr});
        lowBoundStr = upBoundStr;
        lowBoundDate = parseDateTime(upBoundStr);
        upBoundStr = date2Str(addTimeIntervalObj(lowBoundDate, timeIntervalObj));
        loop++;
    }

    EXTENDCLCOUNT[lastSubCLName] = loop;
    return extendCLRangeArray;
}

function getNormalCLExtendName(normalCLArray) {
    // 同一应用下统一类型表
    let CLArray = [];
    let endTimeDate = parseDateTime(ENDTIME);
    for (let i = 0; i < normalCLArray.length; i++) {
        let normalCL = normalCLArray[i];
        try {
            CLArray.push(normalCL);
            // 到最后一个时间命名的普通表
            if (normalCL == LASTNORMALOBJ[normalCL]) {
                let timeIntervalObj;
                if (CLArray.length == 1) {
                    timeIntervalObj = DEFAULTTIMEINTERVALOBJ;
                    let content = "没有找到普通表 [" + normalCL + "] 其他时间命名的表，无法确定时间间隔，使用默认时间间隔: " + JSON.stringify(DEFAULTTIMEINTERVALOBJ);
                    logger.warn(content);
                } else {
                    // 解析名字中的时间
                    let lastNormalCLName = CLArray[CLArray.length - 2].split('.')[1];
                    let CLName = normalCL.split('.')[1];
                    for (let i = 0; i < DATE_FORMAT_ARRAY.length; i++){
                        let dateFormat = DATE_FORMAT_ARRAY[i];
                        // 匹配格式，只考虑 CL，不考虑 CS
                        let lastDate = new Date();
                        let newDate = new Date(lastDate);
                        while (newDate.getTime() < endTimeDate.getTime()) {
                            newDate = new Date(lastDate);
                            let dateStr = dateFormat.replace(new RegExp("\\$YYYY", "i"), newDate.getFullYear());
                            let month = "";
                            let day = "";

                            // 补齐月份和日期的0
                            if (newDate.getMonth() < 9) {
                                month += ('0' + (newDate.getMonth() + 1));
                            } else {
                                month += (newDate.getMonth() + 1);
                            }
                        
                            if (newDate.getDate() < 10) {
                                day += ('0' + newDate.getDate());
                            } else {
                                day += newDate.getDate();
                            }
                            dateStr = dateStr.replace("$MM", month);
                            dateStr = dateStr.replace(new RegExp("\\$dd", "i"), day);

                            let index = CLName.search(dateStr);
                            //logger.info(CLName + " " + dateStr + " " + dateFormat);
                            if (-1 != index) {
                                // 最新分区时间
                                let dateStr1 = dateStr;
                                // 前一个分区时间，要求除时间外，格式需要一致
                                let dateStr2 = lastNormalCLName.substring(index, index + dateStr1.length);
                                timeIntervalObj = getTimeIntervalObj(dateStr2, dateStr1);
                                break;
                            }

                            // 已提前建好了后续的时间分区的子表，这里跳过，直到最后一个时间分区
                            if (-1 != dateFormat.search(new RegExp("\\$dd", "i"))) {
                                lastDate.setDate(lastDate.getDate() + 1);
                            } else if (-1 != dateFormat.search("\\$MM")) {
                                lastDate.setMonth(lastDate.getMonth() + 1);
                            } else if (-1 != dateFormat.search(new RegExp("\\$YYYY", "i"))) {
                                lastDate.setFullYear(lastDate.getFullYear() + 1);
                            } else {
                                let content = "conf/args.js 中 DATE_FORMAT_ARRAY 格式不对，请修改";
                                logger.error(content);
                                throw new Error(content);
                            }
                        }
                    }
                }
                //logger.info(normalCL + " " + JSON.stringify(timeIntervalObj));
                // 从名字判断日期，因为 2020 后缀代表 2020-2021 的数据，ENDTIME 是根据分区时间确定的，所以这里会多出一个分区
                EXTENDNAME[normalCL] = getExtendCLNameArray(normalCL, "",timeIntervalObj);
                CLArray = [];
            }
        } catch (error) {
            logger.except("Failed to get normalCL [" +  CLArray + "] extend cl name", error);
            throw error;
        }
    }
}

function getSubCLInfo(mainCLArray) {
    let CLArray = [];
    for (let i = 0; i < mainCLArray.length; i++) {
        let mainCL = mainCLArray[i];
        try {
            var cursor = db.exec('select CataInfo,ShardingKey from $SNAPSHOT_CATA where IsMainCL=true and Name="' + mainCL + '"');
            var CataInfo = cursor.current().toObj()['CataInfo'];
            // 记录表切分键
            let ShardingKey = Object.keys(cursor.current().toObj().ShardingKey)[0];
            //logger.info(ShardingKey + " " + JSON.stringify(cursor.current().toObj().ShardingKey));
            MAINCLKEY[mainCL] = ShardingKey;
            let timeIntervalObj;
            let lastSubCLName;
            let lastUpBound;
            let lastLowBound;
            let extendCLNameArray = [];
            let extendCLRangeArray = [];
            for (let j = 0; j < CataInfo.length; j++) {
                let subCLName = CataInfo[j].SubCLName;
                let upBound = CataInfo[j].UpBound[ShardingKey];
                let lowBound = CataInfo[j].LowBound[ShardingKey];
                CLArray.push(subCLName);
                // 记录主子表关系
                SUBMAINOBJ[subCLName] = mainCL;
                if (j != 0 && CataInfo[j].UpBound[ShardingKey] instanceof Object) {
                    // 记录 MAX 表
                    MAINMAX[mainCL] = subCLName;
                    // 记录每个表的最后一个子表（除 MAX 表外）
                    LASTSUBOBJ[mainCL] = lastSubCLName;
                    // 获取时间间隔
                    timeIntervalObj = getTimeIntervalObj(lastUpBound, lastLowBound);
                    // 获取时间间隔数组
                    extendCLRangeArray = getextendCLRangeArray(lastSubCLName, CataInfo[j].LowBound[ShardingKey], timeIntervalObj);
                    // 获取扩展分区的名称数组
                    extendCLNameArray = getExtendCLNameArray(lastSubCLName, CataInfo[j].LowBound[ShardingKey], timeIntervalObj);
                } else if (j == CataInfo.length -1) {
                    // 记录每个表的最后一个子表（除 MAX 表外）
                    LASTSUBOBJ[mainCL] = subCLName;
                    // 获取时间间隔
                    timeIntervalObj = getTimeIntervalObj(upBound, lowBound);
                    // 获取时间间隔数组
                    extendCLRangeArray = getextendCLRangeArray(subCLName, upBound, timeIntervalObj);
                    // 获取扩展分区的名称数组
                    extendCLNameArray = getExtendCLNameArray(subCLName, upBound, timeIntervalObj);
                }
                lastSubCLName = subCLName;
                lastUpBound = upBound;
                lastLowBound = lowBound;
            }
            if (lastLowBound.length == 8) {
                let tmpObj = {};
                tmpObj[ShardingKey] = "$YYYY$MM$dd";
                MAINCLKEYOBJ[mainCL] = tmpObj;
            } else if (lastLowBound.length == 14)  {
                let tmpObj = {};
                tmpObj[ShardingKey] = "$YYYY$MM$dd$HH$mm$ss";
                MAINCLKEYOBJ[mainCL] = tmpObj;
            } else {
                MAINCLKEYOBJ[mainCL] = cursor.current().toObj().ShardingKey;
            }

            if (extendCLNameArray.length != extendCLRangeArray.length) {
                let content = "解析扩展子表名与扩展上下界不一致";
                logger.error(content);
                for (let j = 0; j < extendCLNameArray.length; j++) {
                    logger.error("子表名：" + extendCLNameArray[j]);
                }
                for (let j = 0; j < extendCLRangeArray.length; j++) {
                    logger.error("扩展上下界：" + JSON.stringify(extendCLRangeArray[j]));
                }
                throw new Error(content);
            }
            if (extendCLNameArray.length == 0) {
                EXTENDNAME[mainCL] = [];
                EXTENDBOUND[mainCL] = [];
            } else {
                EXTENDNAME[mainCL] = extendCLNameArray;
                EXTENDBOUND[mainCL] = extendCLRangeArray;
            }
        } catch (error) {
            logger.except("Failed to get $SNAPSHOT_CATA with [" +  mainCL + "]", error);
            throw error;
        }
    }
    return CLArray;
}

function outputMainSubCLCSV(fullCLArray) {
    let CLCsv;
    let CLTypeCsv;
    let addCLCsv;

    try {
        removeFile(CLCSV);
        removeFile(CLTYPECSV);
        removeFile(ADDCLCSV);
        // 输出表维度 csv: 表名，应用名，数据域，总记录数，LOB数，总数据量，每个组平均数据量，组上最大数据量，组上最小数据量，最大/最小偏差值，所在数据组，组数
        CLCsv = new File(CLCSV);
        CLCsv.write("CLName,mainCLName,appName,domain,totalRecord,lobs,totalSizeGB,groupAvgSizeGB,groupMaxSizeGB,groupMinSizeGB,Max/Min,groups,groupNum" + '\n');
        // 输出表类型维度 csv: 表类型(主表)，应用名，最后一个时间分区增量数据，时间分区平均数据量，时间分区最大数据量，时间分区最少数据量，最大/最小偏差值，总数据量，总数据组，表个数，总分区数
        CLTypeCsv = new File(CLTYPECSV);
        CLTypeCsv.write("type,appName,lastTimeSizeGB,timeAvgSizeGB,timeMaxSizeGB,timeMinSizeGB,timeMax/timeMin,totalSizeGB,totalGroupNum,CLCount,totalPartNum" + '\n');
        // 输出表类型维度 csv: 应用名，主表名，创建表名，表类型，域，挂载字段，挂载上下界，卸载的 MAX 表，pagesize，logpagesize，partition, indexType
        addCLCsv = new File(ADDCLCSV);
        addCLCsv.write("appName,mainCLName,createCLName,CLType,domain,shardingKey,lowBound,upBound,detachCL,pageSize,lobPageSize,Partition,indexType" + '\n');
    } catch (e) {
        throw e;
    }

    // 表类型(主表)维度
    let subCLCount = 0;
    let CLSizeGB = 0;
    let timeMaxSizeGB = 0;
    let timeMinSizeGB = 0;
    let totalGroupObj = {};

    for (let i = 0; i < fullCLArray.length; i++) {
        let CLCsvLine = [];
        let fullname = fullCLArray[i];
        try {
            let csName = fullname.split('.')[0];
            // 获取主节点 $SNAPSHOT_CS
            let cursor = db.exec('select PageSize,LobPageSize from $SNAPSHOT_CS where Name = "' + csName + '" and nodeselect = "primary"');
            let CSPageSize = cursor.current().toObj().PageSize;
            let CSLobPageSize = cursor.current().toObj().LobPageSize;
            cursor.close();

            // 获取主节点 $SNAPSHOT_CATA
            cursor = db.exec('select Partition from $SNAPSHOT_CATA where Name = "' + fullname + '"');
            let partition;
            if (cursor.current()){
                partition = cursor.current().toObj().Partition;
            }
            cursor.close();

            // 获取主节点 $SNAPSHOT_CL
            cursor = db.exec('select Details from $SNAPSHOT_CL where Name = "' + fullname + '" and nodeselect = "primary" split by Details');

            // 表维度
            let totalSize = 0;
            let lobs = 0;
            let totalSizeGB = 0;
            let groupNum = 0;
            let groupMaxSizeGB = 0;
            let groupMinSizeGB = 0;
            let groups = [];

            while (cursor.next()) {
                // 统计
                let current = cursor.current().toObj().Details
                totalSize += current.TotalRecords;
                lobs += current.TotalLobs;
                let TotalDataPages = current.TotalDataPages;
                let TotalIndexPages = current.TotalIndexPages;
                let PageSize = current.PageSize;
                let TotalDataFreeSpace = current.TotalDataFreeSpace;
                let TotalIndexFreeSpace = current.TotalDataFreeSpace;
                let currentGroupSizeGB = ((TotalDataPages + TotalIndexPages) * PageSize - TotalDataFreeSpace - TotalIndexFreeSpace) / 1024 / 1024 / 1024;
                totalSizeGB += currentGroupSizeGB;
                groupNum++;
                groupMaxSizeGB = groupMaxSizeGB > currentGroupSizeGB ? groupMaxSizeGB : currentGroupSizeGB;
                if (groupMinSizeGB != 0) {
                    groupMinSizeGB = groupMinSizeGB < currentGroupSizeGB ? groupMinSizeGB : currentGroupSizeGB;
                } else {
                    groupMinSizeGB = currentGroupSizeGB;
                }
                groups.push(current.GroupName);
                totalGroupObj[current.GroupName] = 1;
            }

            let mainCL = SUBMAINOBJ[fullname];
            let appName = MAINSUBCLAPPNAMEOBJ[mainCL];
            let domainName = db.getCS(csName).getDomainName();
            // 表维度
            // 表名
            CLCsvLine.push(fullname);
            // 主表名
            CLCsvLine.push(mainCL);
            // 应用名
            CLCsvLine.push(appName);
            // 数据域名
            CLCsvLine.push(domainName);
            // 总记录数
            CLCsvLine.push(totalSize);
            // LOB 数
            CLCsvLine.push(lobs);
            // 总数据量
            CLCsvLine.push(totalSizeGB.toFixed(FIXNUM));
            // 每个组平均数据量
            CLCsvLine.push((totalSizeGB / groupNum).toFixed(FIXNUM));
            // 组上最大数据量
            CLCsvLine.push(groupMaxSizeGB.toFixed(FIXNUM));
            // 组上最小数据量
            CLCsvLine.push(groupMinSizeGB.toFixed(FIXNUM));
            // MAX/MIN
            CLCsvLine.push((groupMaxSizeGB / groupMinSizeGB).toFixed(FIXNUM));
            // 所在数据组
            CLCsvLine.push(groups.join('$'));
            // 组数
            CLCsvLine.push(groupNum);
            // 写入文件
            CLCsv.write(CLCsvLine + '\n');

            // 主表维度
            // 统计
            subCLCount++;
            CLSizeGB += totalSizeGB;
            timeMaxSizeGB = timeMaxSizeGB > totalSizeGB ? timeMaxSizeGB : totalSizeGB;
            if (timeMinSizeGB != 0) {
                timeMinSizeGB = timeMinSizeGB < totalSizeGB ? timeMinSizeGB : totalSizeGB;
            } else {
                timeMinSizeGB = totalSizeGB;
            }
            // 到最后一个时间分区的子表，统计之前的数据（不包含 max 表），然后清空，开始下一张表
            if (fullname == LASTSUBOBJ[mainCL]) {
                let CLTypeCsvLine = [];
                // 表类型
                CLTypeCsvLine.push(CLTYPE[mainCL]);
                // 应用名
                CLTypeCsvLine.push(appName);
                // 最后一个时间分区增量数据（当前分区数据量）
                CLTypeCsvLine.push(totalSizeGB.toFixed(FIXNUM));
                // 时间分区平均数据量
                CLTypeCsvLine.push((CLSizeGB / subCLCount).toFixed(FIXNUM));
                // 时间分区最大数据量
                CLTypeCsvLine.push(timeMaxSizeGB.toFixed(FIXNUM));
                // 时间分区最小数据量
                CLTypeCsvLine.push(timeMinSizeGB.toFixed(FIXNUM));
                // 最大/最小偏差值
                CLTypeCsvLine.push((timeMaxSizeGB / timeMinSizeGB).toFixed(FIXNUM));
                // 总数据量
                CLTypeCsvLine.push(CLSizeGB.toFixed(FIXNUM));
                // 总数据组
                let keys = Object.keys(totalGroupObj);
                let groupArray = [];
                keys.forEach(function (key){groupArray.push(key)});
                CLTypeCsvLine.push(groupArray.length);
                // 总分区数
                if (MAINMAX[mainCL] != "") {
                    INCLUDEMAXRANGE = true;
                    CLTypeCsvLine.push(subCLCount + 1);
                } else {
                    CLTypeCsvLine.push(subCLCount);
                }
                // 写入文件
                CLTypeCsv.write(CLTypeCsvLine + '\n');

                // 清空
                subCLCount = 0;
                CLSizeGB = 0;
                timeMaxSizeGB = 0;
                timeMinSizeGB = 0;
                totalGroupObj = {};

                for (let j = 0; j < EXTENDNAME[mainCL].length; j++) {
                    // 输出 addCL.csv
                    let addCLCsvLine = [];
                    // 应用名
                    addCLCsvLine.push(appName);
                    // 主表名
                    addCLCsvLine.push(mainCL);
                    // 创建表名
                    addCLCsvLine.push(EXTENDNAME[mainCL][j]);
                    // 表类型
                    addCLCsvLine.push(CLTYPE[mainCL]);
                    // 数据域名
                    addCLCsvLine.push(domainName);
                    // 挂载字段
                    addCLCsvLine.push(MAINCLKEY[mainCL]);
                    // 挂载上下界
                    addCLCsvLine.push(EXTENDBOUND[mainCL][j].LowBound);
                    addCLCsvLine.push(EXTENDBOUND[mainCL][j].UpBound);
                    // 需要卸载的 MAX 表
                    addCLCsvLine.push(MAINMAX[mainCL]);
                    // pagesize
                    addCLCsvLine.push(CSPageSize);
                    // lobpagesize
                    addCLCsvLine.push(CSLobPageSize);
                    // partition
                    addCLCsvLine.push(partition);
                    // indexType
                    addCLCsvLine.push("general");
                    // 写入文件
                    addCLCsv.write(addCLCsvLine + "\n");
                }
            }
        } catch (error) {
            logger.except("Failed to get sdb info with [" + fullCLArray[i]  + "]", error);
            throw error;
        }
    }
}

function outputNormalCLCSV(normalCLArray) {
    let CLCsv;
    let CLTypeCsv;
    let addCLCsv;

    try {
        // 输出表维度 csv: 表名，应用名，数据域，总记录数，LOB数，总数据量，每个组平均数据量，组上最大数据量，组上最小数据量，最大/最小偏差值，所在数据组，组数
        CLCsv = new File(CLCSV);
        CLCsv.seek(0, 'e');
        // 输出表类型维度 csv: 表类型(主表)，应用名，最后一个时间分区增量数据，时间分区平均数据量，时间分区最大数据量，时间分区最少数据量，最大/最小偏差值，总数据量，总数据组，总分区数
        CLTypeCsv = new File(CLTYPECSV);
        CLTypeCsv.seek(0, 'e');
        // 输出表类型维度 csv: 应用名，主表名，创建表名，表类型，域，挂载字段，挂载上下界，卸载的 MAX 表，pagesize，logpagesize，partition, indexType
        addCLCsv = new File(ADDCLCSV);
        addCLCsv.seek(0, 'e');
    } catch (e) {
        throw e;
    }

    // 表类型(主表)维度
    let subCLCount = 0;
    let CLSizeGB = 0;
    let timeMaxSizeGB = 0;
    let timeMinSizeGB = 0;
    let totalGroupObj = {};

    for (let i = 0; i < normalCLArray.length; i++) {
        let CLCsvLine = [];
        let fullname = normalCLArray[i];
        try {
            let csName = fullname.split('.')[0];
            // 获取主节点 $SNAPSHOT_CS
            var cursor = db.exec('select PageSize,LobPageSize from $SNAPSHOT_CS where Name = "' + csName + '" and nodeselect = "primary"');
            let CSPageSize = cursor.current().toObj().PageSize;
            let CSLobPageSize = cursor.current().toObj().LobPageSize;
            cursor.close();

            cursor = db.exec('select Partition,ShardingKey from $SNAPSHOT_CATA where Name="' + fullname + '"');
            NORMALCLKEYOBJ[fullname] = cursor.current().toObj().ShardingKey
            // partition
            let partition = cursor.current().toObj().Partition;
            cursor.close();

            // 获取域名
            let domainName = db.getCS(csName).getDomainName();

            // 获取节点 $SNAPSHOT_CL
            cursor = db.exec('select Details from $SNAPSHOT_CL where Name = "' + fullname + '" and nodeselect = "primary" split by Details');

            // 表维度
            let totalSize = 0;
            let lobs = 0;
            let totalSizeGB = 0;
            let groupNum = 0;
            let groupMaxSizeGB = 0;
            let groupMinSizeGB = 0;
            let groups = [];

            while (cursor.next()) {
                // 统计
                let current = cursor.current().toObj().Details
                totalSize += current.TotalRecords;
                lobs += current.TotalLobs;
                let TotalDataPages = current.TotalDataPages;
                let TotalIndexPages = current.TotalIndexPages;
                let PageSize = current.PageSize;
                let TotalDataFreeSpace = current.TotalDataFreeSpace;
                let TotalIndexFreeSpace = current.TotalDataFreeSpace;
                let currentGroupSizeGB = ((TotalDataPages + TotalIndexPages) * PageSize - TotalDataFreeSpace - TotalIndexFreeSpace) / 1024 / 1024 / 1024;
                totalSizeGB += currentGroupSizeGB;
                groupNum++;
                groupMaxSizeGB = groupMaxSizeGB > currentGroupSizeGB ? groupMaxSizeGB : currentGroupSizeGB;
                if (groupMinSizeGB != 0) {
                    groupMinSizeGB = groupMinSizeGB < currentGroupSizeGB ? groupMinSizeGB : currentGroupSizeGB;
                } else {
                    groupMinSizeGB = currentGroupSizeGB;
                }
                groups.push(current.GroupName);
                totalGroupObj[current.GroupName] = 1;
            }

            // 表维度
            // 表名
            CLCsvLine.push(fullname);
            // 主表名
            CLCsvLine.push("");
            // 应用名
            CLCsvLine.push(NORMALCLAPPNAMEOBJ[fullname]);
            // 数据域名
            CLCsvLine.push(db.getCS(csName).getDomainName());
            // 总记录数
            CLCsvLine.push(totalSize);
            // LOB 数
            CLCsvLine.push(lobs);
            // 总数据量
            CLCsvLine.push(totalSizeGB.toFixed(FIXNUM));
            // 每个组平均数据量
            CLCsvLine.push((totalSizeGB / groupNum).toFixed(FIXNUM));
            // 组上最大数据量
            CLCsvLine.push(groupMaxSizeGB.toFixed(FIXNUM));
            // 组上最小数据量
            CLCsvLine.push(groupMinSizeGB.toFixed(FIXNUM));
            // MAX/MIN
            CLCsvLine.push((groupMaxSizeGB / groupMinSizeGB).toFixed(FIXNUM));
            // 所在数据组
            CLCsvLine.push(groups.join('$'));
            // 组数
            CLCsvLine.push(groupNum);
            // 写入文件
            CLCsv.write(CLCsvLine + '\n');

            // 主表维度（普通表模拟）
            // 统计
            subCLCount++;
            CLSizeGB += totalSizeGB;
            timeMaxSizeGB = timeMaxSizeGB > totalSizeGB ? timeMaxSizeGB : totalSizeGB;
            if (timeMinSizeGB != 0) {
                timeMinSizeGB = timeMinSizeGB < totalSizeGB ? timeMinSizeGB : totalSizeGB;
            } else {
                timeMinSizeGB = totalSizeGB;
            }
            // 到最后一个时间分区的子表，统计之前的数据（不包含 max 表），然后清空，开始下一张表
            if (fullname == LASTNORMALOBJ[fullname]) {
                let CLTypeCsvLine = [];
                // 表类型
                CLTypeCsvLine.push(CLTYPE[fullname]);
                // 应用名
                CLTypeCsvLine.push(NORMALCLAPPNAMEOBJ[fullname]);
                // 最后一个时间分区增量数据（当前分区数据量）
                CLTypeCsvLine.push(totalSizeGB.toFixed(FIXNUM));
                // 时间分区平均数据量
                CLTypeCsvLine.push((CLSizeGB / subCLCount).toFixed(FIXNUM));
                // 时间分区最大数据量
                CLTypeCsvLine.push(timeMaxSizeGB.toFixed(FIXNUM));
                // 时间分区最小数据量
                CLTypeCsvLine.push(timeMinSizeGB.toFixed(FIXNUM));
                // 最大/最小偏差值
                CLTypeCsvLine.push((timeMaxSizeGB / timeMinSizeGB).toFixed(FIXNUM));
                // 总数据量
                CLTypeCsvLine.push(CLSizeGB.toFixed(FIXNUM));
                // 总数据组
                let keys = Object.keys(totalGroupObj);
                let groupArray = [];
                keys.forEach(function (key){groupArray.push(key)});
                CLTypeCsvLine.push(groupArray.length);
                // 总分区数
                CLTypeCsvLine.push(subCLCount);
                // 写入文件
                CLTypeCsv.write(CLTypeCsvLine + '\n');

                // 清空
                subCLCount = 0;
                CLSizeGB = 0;
                timeMaxSizeGB = 0;
                timeMinSizeGB = 0;
                totalGroupObj = {};

                for (let j = 0; j < EXTENDNAME[fullname].length - 1; j++) {
                    // 输出 addCL.csv
                    let addCLCsvLine = [];
                    // 应用名
                    addCLCsvLine.push(NORMALCLAPPNAMEOBJ[fullname]);
                    // 主表名
                    addCLCsvLine.push("");
                    // 创建表名
                    addCLCsvLine.push(EXTENDNAME[fullname][j]);
                    // 表类型
                    addCLCsvLine.push(CLTYPE[fullname]);
                    // 数据域名
                    addCLCsvLine.push(domainName);
                    // 挂载字段
                    addCLCsvLine.push("");
                    // 挂载上下界
                    addCLCsvLine.push("");
                    addCLCsvLine.push("");
                    // 需要卸载的 MAX 表
                    addCLCsvLine.push("");
                    // pagesize
                    addCLCsvLine.push(CSPageSize);
                    // lobpagesize
                    addCLCsvLine.push(CSLobPageSize);
                    // partition
                    addCLCsvLine.push(partition);
                    // indexType
                    addCLCsvLine.push("general");
                    // 写入文件
                    addCLCsv.write(addCLCsvLine + "\n");
                }
            }
        } catch (error) {
            logger.except("Failed to get normalCL [" + normalCLArray[i]  + "] info", error);
            throw error;
        }
    }
}

function removeFile(filename) {
    try {
        if (File.exist(filename)) {File.remove(filename);}
    } catch (error) {
        throw error;
    }
}

function outputModel() {
    let modelFile;

    try {
        removeFile(CURRENTMODELJSON);
        modelFile = new File(CURRENTMODELJSON);
    } catch (e) {
        throw e;
    }

    let modelJson = {};
    // 只有有一个有，这里就是 true
    modelJson["include_max_range"] = INCLUDEMAXRANGE;
    let main_cl_array = [];
    let normal_cl_array = [];
    let sub_cl_array = [];
    let CLObj = {};
    let mainCLModelArray = [];
    let subCLModelArray = [];
    let normalCLModelArray = [];

    // 构造数据
    for (let key in CLFINDCOND) {
        if (-1 != mainCLModelArray.indexOf(CLFINDCOND[key]) || -1 != normalCLModelArray.indexOf(CLFINDCOND[key])) {
            continue;
        } else {
            CLObj['model'] = CLFINDCOND[key];
            let shardingkey = {};
            // 主表
            if (MAINCLKEY[key] != undefined) {
                CLObj['type'] = CLTYPE[key];
                mainCLModelArray.push(CLFINDCOND[key]);
                shardingkey["type"] = "range";
                shardingkey["key"] = MAINCLKEYOBJ[key];
                //logger.info(JSON.stringify(MAINCLKEYOBJ[key]));
                CLObj['shardingkey'] = shardingkey;
                main_cl_array.push(CLObj);
            // 普通表
            } else {
                CLObj['type'] = CLTYPE[key];
                normalCLModelArray.push(CLFINDCOND[key]);
                shardingkey["type"] = "hash";
                shardingkey["key"] = NORMALCLKEYOBJ[key];
                CLObj['shardingkey'] = shardingkey;
                normal_cl_array.push(CLObj);
            }
        }
        CLObj = {};
    }
    let key;

    // 子表
    try {
        for (key in EXTENDNAME) {
            let appName = MAINSUBCLAPPNAMEOBJ[key];
            if (appName == undefined) {continue;}
            //logger.info(key + " --- " + LASTSUBOBJ[key]);
            let dateStr = EXTENDNAMEDATESTR[LASTSUBOBJ[key]].dateStr;
            let dateFormat = EXTENDNAMEDATESTR[LASTSUBOBJ[key]].dateFormat;
            let model = LASTSUBOBJ[key].replace(new RegExp(appName, 'g'), "$APPNAME").replace(new RegExp(dateStr, 'g'), dateFormat);
            model.replace(new RegExp("\\$YYYYmmdd", "i"), "$DATE");
            model = model.replace(new RegExp("\\$YYYYmm", "i"), "$DATE");
            model = model.replace(new RegExp("\\$YYYY", "i"), "$DATE");
            if (-1 != subCLModelArray.indexOf(model)) {
                continue;
            } else {
                subCLModelArray.push(model);
                CLObj['model'] = model;
                CLObj['type'] = CLTYPE[key];
                let shardingkey = {};
                shardingkey["type"] = "hash";
                // 最后一张子表
                let cursor = db.exec('select ShardingKey from $SNAPSHOT_CATA where Name="' + LASTSUBOBJ[key] + '"');
                shardingkey["key"] = cursor.current().toObj().ShardingKey;
                CLObj['shardingkey'] = shardingkey;
                sub_cl_array.push(CLObj);
            }
            CLObj = {};
        }
    } catch (error) {
        logger.except("Failed to get $SNAPSHOT_CATA with [" + LASTSUBOBJ[key]  + "]", error);
        throw error;
    }

    modelJson['main_cl'] = main_cl_array;
    modelJson['sub_cl'] = sub_cl_array;
    modelJson['normal_cl'] = normal_cl_array;
    modelFile.write(JSON.stringify(modelJson, null, 2) + '\n');
    modelFile.close();
}

//全部不匹配打一个，匹配到一个也打一个
function checkModel() {
    try {
        let cmd = new Cmd();
        let modelFileArray = cmd.run("ls", MODELDIR).split("\n");
        //logger.info(modelFileArray);
        for (let i = 0; i < modelFileArray.length; i++) {
            let fileName = modelFileArray[i];
            if (fileName == "") {continue;}
            let baseModelFile = MODELDIR + fileName;
            //logger.info(baseModelFile);
            let ret = checkOneModel(baseModelFile);
            if (ret || ret == undefined) {
                logger.info("当前模型与标准模型 " + baseModelFile + " 符合");
                break;
            } else {
                logger.info("当前模型与标准模型 " + baseModelFile + " 不符合");
            }
        }
    } catch (error) {
        
    }
}

function checkModelCLArray(baseCLArray, curCLArray){
    if (baseCLArray.length != curCLArray.length) {
        return false;
    }
    for (let i = 0; i < baseCLArray.length; i++) {
        let isAccord = false;
        let base = baseCLArray[i];
        for (let j = 0; j < curCLArray.length; j++) {
            let cur = curCLArray[j];
            if (base.type == cur.type) {
                isAccord = true;
                if (base.model != cur.model) { return false; }
                if (base.shardingkey.type != cur.shardingkey.type) { return false; }
                // 待补充
                //if (base.shardingkey.key != cur.shardingkey.key) { return false; }
            }
        }
        if (!isAccord) {return false};
    }
    return true;
}

function checkOneModel(baseModelFile) {
    try {
        let cmd = new Cmd();
        let baseModelObj = JSON.parse(cmd.run("cat", baseModelFile));
        let curModelObj = JSON.parse(cmd.run("cat", CURRENTMODELJSON));
        if (typeof baseModelObj !== "object") {
            let content = baseModelObj + " 文件格式错误";
            logger.error(content);
            throw new Error(content);
        }

        if (typeof curModelObj !== "object") {
            let content = curModelObj + " 文件格式错误";
            logger.error(content);
            throw new Error(content);
        }

        // 对比 mainCL 
        if (!checkModelCLArray(baseModelObj.main_cl, curModelObj.main_cl)) {return false;}
        if (!checkModelCLArray(baseModelObj.sub_cl, curModelObj.sub_cl)) {return false;}
        if (!checkModelCLArray(baseModelObj.normal_cl, curModelObj.normal_cl)) {return false;}

        return true;
    } catch (error) {
        logger.except("Failed to get check model with [" + modelFile  + "]", error);
        throw error;
    }
}

function outputFile(fullSubCLArray, normalCLArray) {
    outputMainSubCLCSV(fullSubCLArray);
    logger.info("主子表信息输出完成");
    outputNormalCLCSV(normalCLArray);
    logger.info("普通表信息输出完成");
    logger.info("表维度信息文件: " + CLCSV);
    logger.info("表类型维度信息文件: " + CLTYPECSV);
    outputModel();
    logger.info("模型信息输出完成");
    logger.info("当前模型信息文件: " + CURRENTMODELJSON);
    checkModel();
    logger.info("模型匹配完成");
}

function getSdbInfo() {
    // 获取主表名
    let mainCLArray = findCL(MAIN_CL_ARRAY, "MainSubCL");
    logger.info("获取主表完成，数量为：" + mainCLArray.length);

    // 获取普通表名
    let normalCLArray = findCL(NORMAL_CL_ARRAY, "NormalCL");
    if (NORMAL_CL_ARRAY.length != 0) {
        logger.info("获取普通表完成，数量为：" + normalCLArray.length);
        // 将表排序，预期是同一类型的表按照时间后缀排在一起
        normalCLArray.sort();
    }
    // 获取普通表扩展名
    getNormalCLExtendName(normalCLArray);

    // 获取全量子表信息
    let fullSubCLArray = getSubCLInfo(mainCLArray);
    logger.info("获取全部表完成，数量为：" + fullSubCLArray.length)

    // 输出文件
    outputFile(fullSubCLArray,normalCLArray);
}

/*
    start
*/

function main() {
    getSdbInfo();
}

main();