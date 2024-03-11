// 执行为 run，本工具用于检查，没有回滚功能MAIN
if (typeof MODE == "undefined" || MODE == null || MODE == "") {
    var MODE = "test";
}

// ------------ 外部参数，大部分情况下不需要修改 ------------
// 可实现从客户通过内置 SQL 拿回的 $SNAPSHOT_CL,$SNAPSHOT_CS,$SNAPSHOT_CATA 到本地处理数据
var USELOCAL = false;
// 如果 useLocal 为 true，需要修改下面三个表名为对应已有数据的表名
var LOCALCATA = "db1.cata";
var LOCALCL = "db1.cl";
var LOCALCS = "db1.cs";

var SNAPSHOTCATANAME = "SNAPSHOT_CATA";
var SNAPSHOTCLNAME = "SNAPSHOT_CL";
var SNAPSHOTCSNAME = "SNAPSHOT_CS";

// 中途临时存储数据的 CL 名
// 存储从 SNAPSHOT_CATA 正则匹配命中的 CL 名的 CATA 信息
var MATCHCATANAME = "MATCH_CATA"
// 存储从 SNAPSHOT_CL 匹配命中的 CL 名的 CL 信息
var MATCHCLNAME = "MATCH_CL"
// 混合 SNAPSHOT_CS 和 SNAPSHOT_Cl 配置的表
var HYBRIDCSCLNAME = "HYBRID_CS_CL"
// CL 每个组的数据量大小
var GROUPSIZENAME = "GROUP_SIZE";
// CL 级数据量大小，包括 MAX MIN
var CLSIZENAME = "CL_SIZE";
// 记录了表与最后一张表关系
var LASTCLNAME = "LAST_CL";
// 临时表，用于各种临时数据存储，一个地方用完需要马上清除数据
var TMPCLNAME = "TMP";
// 存储主表名和对应子表的查找条件
var SUBCLFINDCONDCLNAME = "SUB_CL_FIND_COND"; 
// 存储 CS 与 DOMAIN 的关系;
var CSDOAMINNAME = "CS_DOMAIN";
// 记录了主表与MAX表的关系
var MAINMAXNAME = "MAIN_MAX"
// 扩展表信息
var EXTENDCLNAME = "EXTEND_CL";
// 最终准输出的表
var OUTOUTCLNAME = "OUTPUT";

var DETAILINFOCLNAME = "INFO_DETAIL";

// 每收集多少张表就打印一次信息
var CLOUTPUTONCE = 50;
// 每收集输出多少条记录到文件就打印一次信息
var INFOOUTPUTONCE = 200;

// 计算数值后保留多少位小数
var FIXNUM = 4;

// MAX - MIN 差大于此值才会计算 MAX/MIN，单位为 M，默认值为 100，正式使用不需要改，留个变量设置为 0 用于测试
var MAX_MIN = 100;

// 读取的文件路径
var CONFIGJSON = "conf/config.json";
var MODELDIR = "conf/model/";

// 输出的文件路径
var CLCSV = "output/infomation_by_cl.csv";
var CLTYPECSV = "output/infomation_by_cl_type.csv";
var CURRENTMODELJSON = "output/current_model.json";
var ADDCLCSV = "conf/add_cl.csv";

// 日期长度，用于把主表 range 切分键的时间字符串转换为时间，默认兼容 20240101 和 20240101000000 两种格式
var DATEFORMAT = { year: 4, month: 6, day: 8, hour: 10, minute: 12, second: 14};

// 普通表默认时间间隔，用于仅有一张普通 LOB 表，无法自动推算出间隔时；如果是分区表，会从上下界推算，不使用该值
var DEFAULTTIMEINTERVALOBJ = {"diffYear": 1, "diffMonth": 0, "diffDay": 0};

// ------------ 内部变量，不要修改 ------------
var TOOL = "check_model";
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
    let sizeObj = {};
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
        } else if (clArray.length < 2) {
            let content = "bin/args.js 文件中 [" + JSON.stringify(CLNameObj) + "] 格式有问题，目前必须要求 CL 中有一个 $APPNAME";
            logger.error(content);
            throw new Error(content);
        }
        let cs_prefix;
        let cs_suffix;
        let cl_prefix = clArray[0];
        let cl_suffix = clArray[1];
        let regexStr = "";

        if (csArray.length == 2) {
            cs_prefix = csArray[0];
            cs_suffix = csArray[1];
            regexStr = "^" + cs_prefix + "([^\.]*)" + cs_suffix + "\." + cl_prefix + "\\1" + cl_suffix + "$"
        } else {
            cs_prefix = csArray[0];
            cs_suffix = "";
            regexStr = "^" + cs_prefix + cs_suffix + "\." + cl_prefix + "(.*?)" + cl_suffix + "$"
        }
        
        //logger.debug(regexStr);
        let condArray = changDate(regexStr);
        // 先获取每个条件的表，并保存
        // 如果不同的条件匹配到相同的表，说明一个更大的匹配条件包括了一个小的匹配条件，那么总匹配表数少的条件会是匹配范围更小的，所以这张表会认为是总匹配表数少的
        let snapshotCataCL = db.getCS(DATACS).getCL(SNAPSHOTCATANAME);
        let matchCL = db.getCS(DATACS).getCL(MATCHCATANAME);
        for (let j = 0; j < condArray.length; j++) {
            let cursor;
            try {
                cursor = snapshotCataCL.find({Name: {"$regex": condArray[j]}});
                //cursor = db.list(4,{Name: {"$regex": condArray[j]}},{Name:1});
            } catch (error) {
                logger.except("查询 " + DATACS + "." + SNAPSHOTCATANAME + ".find(4,{Name:{\"$regex\":\"" + condArray[j] + "\"}}.{Name:1}}) 失败", error);
                throw error;
            }
    
            let size = 0;
            while (cursor.next()) {
                size++;
                let current = cursor.current().toObj();
                let CLName = current['Name'];
                let appName = CLName.replace(new RegExp(condArray[j]),"$1");
                // 写入表
                try {
                    //logger.debug(appName + " " + condArray[j]);
                    current['appName'] = appName;
                    current['findCondition'] = CLNameObj[CLtype];
                    current['modelType'] = CLtype;
                    //current['isFind'] = false;
                    delete current._id;
                    matchCL.insert(current);
                } catch (error) {
                    logger.except("往表 [" + DATACS + "." + MATCHCATANAME + "] 中插入数据失败", error);
                    throw error;
                }
            }
            //logger.debug(condArray[j] + " " + size);
            sizeObj[condArray[j]] = size;
            cursor.close();
        }
    }

    //logger.debug(JSON.stringify(sizeObj));
    // 查找是否有不同的条件匹配到相同的表
    let matchCL = db.getCS(DATACS).getCL(MATCHCATANAME);
    let execTime = 1;
    // 一次只能处理两个匹配条件，如果有多个匹配条件，需要处理多次，直到查不出来，比如一张表被三个条件同时匹配，就会循环，直到没有匹配
    while (execTime != 0) {
        let cursor;
        execTime = 0;
        try {
            cursor = db.exec('select t1.Name,t1.findCondition as cond1,t2.findCondition as cond2 from ' + DATACS + "." + MATCHCATANAME + ' as t1 inner join ' + DATACS + "." + MATCHCATANAME + ' as t2 on t1.Name = t2.Name where t1.findCondition <> t2.findCondition group by t1.Name');
        } catch (error) {
            logger.except("查找是否有不同的条件匹配到相同的表失败", error);
            throw error;
        }
        while(cursor.next()) {
            execTime++;
            let current = cursor.current().toObj();
            try {
                //logger.debug(current.cond1 + " " + current.cond2);
                // 优先匹配少的
                if (sizeObj[current.cond1] > sizeObj[current.cond2]) {
                    //logger.debug(current.CLName + " 1 " + current.cond1);
                    matchCL.remove({$and:[{"Name":{$et:current.CLName}},{"findCondition":{$et:current.cond1}}]});
                } else if (sizeObj[current.cond1] > sizeObj[current.cond2]) {
                    //logger.debug(current.CLName + " 2 " + current.cond1);
                    matchCL.remove({$and:[{"Name":{$et:current.CLName}},{"findCondition":{$et:current.cond1}}]});
                // 如果匹配数一样（应该不可能），选长的
                } else if (sizeObj[current.cond1] == sizeObj[current.cond2] && current.cond1.length < current.cond2.length) {
                    //logger.debug(current.CLName + " 3 " + current.cond1);
                    matchCL.remove({$and:[{"Name":{$et:current.CLName}},{"findCondition":{$et:current.cond1}}]});
                // 选第二个(短)
                } else {
                    //logger.debug(current.CLName + " 4 " + current.cond2);
                    matchCL.remove({$and:[{"Name":{$et:current.CLName}},{"findCondition":{$et:current.cond2}}]});
                }
            } catch (error) {
                logger.except("删除表 [" + DATACS + "." + MATCHCATANAME + "] 中重复匹配数据失败", error);
                throw error;
            }
        }
        cursor.close();
    }

    let retSize = 0;
    try {
        switch (type) {
            case "MainSubCL":
                retSize = db.exec('select Name from ' + DATACS + "." + MATCHCATANAME + ' where IsMainCL = true').size();
                break;
            case "NormalCL":
                retSize = db.exec('select Name from ' + DATACS + "." + MATCHCATANAME + ' where IsMainCL is null and MainCLName is null').size();
                break;
            default:
                break;
        }
    } catch (error) {
        logger.except("从表 [" + DATACS + "." + MATCHCATANAME + "] 中或者表个数失败", error);
        throw error;
    }
    return retSize;
}

// to YYYYMMddHHmmss
function date2Str(date, more) {
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

    if (more != undefined && more != "" && more != false) {
        if (date.getHours() < 10) {
            dateStr += ('0' + date.getHours());
        } else {
            dateStr += date.getHours();
        }

        if (date.getMinutes() < 10) {
            dateStr += ('0' + date.getMinutes());
        } else {
            dateStr += date.getMinutes();
        }

        if (date.getSeconds() < 10) {
            dateStr += ('0' + date.getSeconds());
        } else {
            dateStr += date.getSeconds();
        }
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
function getExtendCLNameArray(mainCL, lastSubCLName, lastUpBound, timeIntervalObj) {
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

                if (mainCL != "") {
                    //logger.debug(mainCL)
                    try {
                        // 对 $APPNAME 的替换后移到输出时，这里拿 $APPNAME 不方便
                        //let findCondition = lastSubCLName.replace(new RegExp(current.appName, 'g'), "$APPNAME").replace(new RegExp(oldDateStr, 'g'), modelDateFormat);
                        let findCondition = lastSubCLName.replace(new RegExp(oldDateStr, 'g'), modelDateFormat);
                        db.getCS(DATACS).getCL(SUBCLFINDCONDCLNAME).insert({"mainCLName":mainCL,"subCLFindCondtiton":findCondition});
                    } catch (error) {
                        logger.except("往表 [" + DATACS + "." + SUBCLFINDCONDCLNAME + "] 中插入子表查询条件失败", error);
                        throw error;
                    }
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

function getNormalCLExtendName() {
    // 同一应用下同一类型表
    let CLArray = [];
    let endTimeDate = parseDateTime(ENDTIME);

    // 使用 TMPCLNAME 对进行排序查找最后一个表，先把同一模型、同一应用的表放进去
    try {
        db.execUpdate('insert into ' + DATACS + "." + TMPCLNAME + ' select CLName as value,appName,modelType from ' + DATACS + "." + MATCHCATANAME + ' where IsMainCL is null and MainCLName is null');
    } catch (error) {
        logger.except("查询表 [" + DATACS + "." + MATCHCATANAME + "] 数据插入到 [" + DATACS + "." + TMPCLNAME + "] 失败", error);
        throw error;
    }

    // 聚集后的 $first 不受 order by 影响，但是 $max 和 $min 可以取到，同时找出倒数第一和第二张表，可能会找不出
    //db.exec('select a.lastCLName,max(b.value) as secondCLName,b.appName,b.modelType from (select max(value) as lastCLName,appName,modelType from MODEL.SORT group by appName,modelType) as a inner join MODEL.SORT as b on a.appName = b.appName and a.modelType = b.modelType where a.lastCLName <> b.value group by b.appName,b.modelType')
    try {
        db.execUpdate('insert into ' + DATACS + "." + LASTCLNAME + ' select a.lastCLName,max(b.value) as secondCLName,b.appName,b.modelType from (select max(value) as lastCLName,appName,modelType from ' + DATACS + "." + TMPCLNAME + ' group by appName,modelType) as a inner join ' + DATACS + "." + TMPCLNAME + ' as b on a.appName = b.appName and a.modelType = b.modelType where a.lastCLName <> b.value group by b.appName,b.modelType')
    } catch (error) {
        logger.except("查询表 [" + DATACS + "." + TMPCLNAME + "] 数据插入到 [" + DATACS + "." + LASTCLNAME + "] 失败", error);
        throw error;
    }

    // if (CLArray.length == 1) {
    //     timeIntervalObj = DEFAULTTIMEINTERVALOBJ;
    //     let content = "没有找到普通表 [" + normalCL + "] 其他时间命名的表，无法确定时间间隔，使用默认时间间隔: " + JSON.stringify(DEFAULTTIMEINTERVALOBJ);
    //     logger.warn(content);
    // }

    try {
        let cursor = db.exec('select * from ' + DATACS + "." + LASTCLNAME + ' where appName is not null');
        while (cursor.next()) {
            let current = cursor.current().toObj();
            // 解析名字中的时间
            let secondCLName = current.secondCLName.split('.')[1];
            let lastCLName = current.lastCLName;
            let lastCLNameCL = lastCLName.split('.')[1];
            let isFind = false;
            for (let i = 0; i < DATE_FORMAT_ARRAY.length; i++){
                let dateFormat = DATE_FORMAT_ARRAY[i];
                // 匹配格式，只考虑 CL，不考虑 CS
                let lastDate = new Date();
                let newDate = new Date(lastDate);
                if (isFind) {
                    break;
                }
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

                    let index = lastCLNameCL.search(dateStr);
                    //logger.info(lastCLNameCL + " " + dateStr + " " + dateFormat);
                    if (-1 != index) {
                        // 最新分区时间
                        let dateStr1 = dateStr;
                        // 前一个分区时间，要求除时间外，格式需要一致
                        let dateStr2 = secondCLName.substring(index, index + dateStr1.length);
                        //logger.debug(dateStr2 + "|" + dateStr1);
                        timeIntervalObj = getTimeIntervalObj(dateStr2, dateStr1);
                        //logger.debug(JSON.stringify(timeIntervalObj));
                        isFind = true;
                        break;
                    }

                    // 已提前建好了后续的时间分区的子表，这里跳过，直到最后一个时间分区
                    // 没有考虑到往前的情况，即当前时间没有子表
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
            if (timeIntervalObj == undefined || timeIntervalObj == "") {
                let content = "无法获取普通表 [" +  CLArray + "] 的时间间隔: " + JSON.stringify(timeIntervalObj);
                logger.error(content);
                throw new Error(content);
            }
            //logger.debug(normalCL + " " + JSON.stringify(timeIntervalObj));
            // 从名字判断日期，因为 2020 后缀代表 2020-2021 的数据，ENDTIME 是根据分区时间确定的，所以这里会多出一个分区
            let extendArray = getExtendCLNameArray("", lastCLName, "",timeIntervalObj);

            if (extendArray.length == 0 && endTimeDate.getTime()) {
                let content = "无法生成普通表 [" + lastCLName + "] 的时间扩展表名，请检查 bin/args.js 中 ENDTIME 是否合理，或者是表名中没有可扩展的时间字符串";
                logger.warn(content);
            } else {
                try {
                    let insertArray = [];
                    let extendcl = db.getCS(DATACS).getCL(EXTENDCLNAME);
                    for (let i = 0; i < extendArray.length; i++) {
                        insertArray.push({"prevCLName":lastCLName,"createCLName":extendArray[i],"upBound":"","lowBound":""});
                    }
                    extendcl.insert(insertArray);
                } catch (error) {
                    logger.except("向表 [" +  EXTENDCLNAME + "] 中插入数据失败", error);
                    throw error;
                }
            }
        }
        cursor.close();
    } catch (error) {
        logger.except("查询表 [" + DATACS + "." + LASTCLNAME + "] 数据，获取时间间隔失败", error);
        throw error;
    }

    try {
        db.getCS(DATACS).getCL(TMPCLNAME).truncate();
    } catch (error) {
        logger.except("删除表 [" + DATACS + "." + TMPCLNAME + "] 数据失败", error);
        throw error;
    }
}

// 找 MAX 分区和最后一个分区不可避免的要遍历所有子表，因为挂载时可能是无序的
function getFullSubCLInfo() {
    let sortcl;
    try {
        sortcl = db.getCS(DATACS).getCL(TMPCLNAME);
    } catch (error) {
        logger.except("获取表 [" + DATACS + "." + TMPCLNAME + "] 失败", error);
        throw error;
    }

    let mainmaxcl;
    try {
        mainmaxcl = db.getCS(DATACS).getCL(MAINMAXNAME);
    } catch (error) {
        logger.except("获取表 [" + DATACS + "." + MAINMAXNAME + "] 失败", error);
        throw error;
    }

    let subCLsize = 0;
    try {
        let cursor = db.exec('select Name,CataInfo,ShardingKey,ShardingType,appName from ' + DATACS + '.' + MATCHCATANAME + ' where IsMainCL = true');
        while (cursor.next()) {
            let current = cursor.current().toObj();
            let mainCL = current.Name;
            let CataInfo = current.CataInfo;
            let appName = current.appName;
            //logger.debug(mainCL);
            // 记录表切分键字符串
            let ShardingKey = Object.keys(current.ShardingKey)[0];
            //logger.info(ShardingKey + " " + JSON.stringify(cursor.current().toObj().ShardingKey));
            for (let j = 0; j < CataInfo.length; j++) {
                subCLsize++;
                let subCLName = CataInfo[j].SubCLName;
                //logger.debug(' ' + subCLsize + ' ' + subCLName);
                let upBound = CataInfo[j].UpBound[ShardingKey];
                let lowBound = CataInfo[j].LowBound[ShardingKey];
                try {
                    // 补充子表 CATA 信息
                    //logger.debug('insert into ' + DATACS + '.' + MATCHCATANAME + ' select a.Name,a.MainCLName,a.CataInfo,a.ShardingKey,a.ShardingType,b.appName from (select Name,CataInfo,ShardingKey,ShardingType,MainCLName from ' + DATACS + '.' + SNAPSHOTCATANAME + ' where Name = "' + subCLName + '") as a inner join ' + DATACS + '.' + MATCHCATANAME + ' as b on a.MainCLName = b.Name')
                    db.execUpdate('insert into ' + DATACS + '.' + MATCHCATANAME + ' select a.Name,a.MainCLName,a.CataInfo,a.ShardingKey,a.ShardingType,b.appName from (select Name,CataInfo,ShardingKey,ShardingType,MainCLName from ' + DATACS + '.' + SNAPSHOTCATANAME + ' where Name = "' + subCLName + '") as a inner join ' + DATACS + '.' + MATCHCATANAME + ' as b on a.MainCLName = b.Name')
                } catch (error) {
                    logger.except("往表 [" + DATACS + "." + MATCHCATANAME + "] 中补充子表 CATA 数据失败", error);
                    throw error;
                }
                //logger.debug(JSON.stringify(CataInfo[j].UpBound[ShardingKey]));
                // 存在 $maxKey 或 $minKey
                if (CataInfo[j].UpBound[ShardingKey] instanceof Object) {
                    let keyStr = Object.keys(CataInfo[j].UpBound[ShardingKey])[0];
                    if (keyStr == "$maxKey") {
                        // 插入表记录 maxclname
                        try {
                            mainmaxcl.insert({MainCLName:mainCL,MaxCLName:subCLName});
                        } catch (error) {
                            logger.except("往表 [" + DATACS + "." + MAINMAXNAME + "] 中插入 MAINCL 与 MAXCL 关系数据失败", error);
                            throw error;
                        }
                    } else if (keyStr == "$minKey") {
                    } else {
                        let content = "表 [" + subCLName + "] 存在未知类型的 shardingKey: " + keyStr;
                        logger.error(content);
                        throw new Error(content);
                    }
                } else {
                    // 把 shardingkey 的值插入临时表，然后排序找出最后一个分区名
                    try {
                        // value 有索引，取 MAX 值快
                        //logger.debug(JSON.stringify(current.ShardingKey));
                        sortcl.insert({"CLName":subCLName,"shardingKey":ShardingKey,"shardingKeyObj":JSON.stringify(current.ShardingKey),"value":upBound,"lowBound":lowBound,"mainCLName":mainCL});
                    } catch (error) {
                        logger.except("往临时表 [" + DATACS + "." + TMPCLNAME + "] 中插入数据失败", error);
                        throw error;
                    }
                }
            }

            // 找到最后一个分区的，并插入到 lastcl 表中
            try {
                db.execUpdate('insert into ' + DATACS + "." + LASTCLNAME + ' \
                select t1.CLName as lastCLName,t1.upBound,t1.lowBound,t2.CLName as secondCLName,t2.mainCLName,t2.shardingKey from \
                (select b.CLName,a.upBound,b.lowBound from (select max(value) as upBound from ' + DATACS + "." + TMPCLNAME + ' ) \as a inner join ' + DATACS + "." + TMPCLNAME + ' as b on a.upBound = b.value) \
                as t1 inner join ' + DATACS + "." + TMPCLNAME + ' as t2 on t1.lowBound = t2.value')
            } catch (error) {
                logger.except("查询表 [" + DATACS + "." + TMPCLNAME + "] 数据插入到 [" + DATACS + "." + LASTCLNAME + "] 失败", error);
                throw error;
            }

            // 清空临时表
            try {
                db.getCS(DATACS).getCL(TMPCLNAME).truncate();
            } catch (error) {
                logger.except("删除临时表 [" + DATACS + "." + TMPCLNAME + "] 数据失败", error);
                throw error;
            }
        }
        cursor.close();

        try {
            // 主子表字段为 lastCLName,upBound,lowBound 普通表字段为 lastCLName,appName,modelType
            // 查找最后一个分区表
            cursor = db.exec('select lastCLName,shardingKey,shardingKeyObj,lowBound,upBound,mainCLName,secondCLNamE from ' + DATACS + "." + LASTCLNAME + ' where mainCLName is not null');
            let timeIntervalObj;
            let extendCLNameArray = [];
            let extendCLRangeArray = [];
            while (cursor.next()) {
                let current = cursor.current().toObj();
                let upBound = current.upBound;
                let lowBound = current.lowBound;
                let lastCLName = current.lastCLName;
                let mainCLName = current.mainCLName;
                let shardingKeyObj = current.shardingKeyObj;
                //logger.debug(shardingKeyObj);
                let secondCLName = current.secondCLName;
                let shardingKey = current.shardingKey;
                // 获取时间间隔
                timeIntervalObj = getTimeIntervalObj(upBound, lowBound);
                if (timeIntervalObj == undefined || timeIntervalObj == "") {
                    let content = "通过 UpBound: " + upBound + " 和 LowBound: " + lowBound + " 获取的时间间隔有误: " + JSON.stringify(timeIntervalObj);
                    logger.error(content);
                    throw new Error(content);
                }
                // 获取时间间隔数组
                extendCLRangeArray = getextendCLRangeArray(lastCLName, upBound, timeIntervalObj);
                // 获取扩展分区的名称数组
                extendCLNameArray = getExtendCLNameArray(mainCLName, lastCLName, upBound, timeIntervalObj);

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

                try {
                    let insertArray = [];
                    let extendcl = db.getCS(DATACS).getCL(EXTENDCLNAME);
                    let insertShardingKeyObj = {};
                    if (lowBound.length == 8) {
                        insertShardingKeyObj[shardingKey] = "$YYYY$MM$dd";
                    } else if (lowBound.length == 14)  {
                        let tmpObj = {};
                        insertShardingKeyObj[shardingKey] = "$YYYY$MM$dd$HH$mm$ss";
                    } else {
                        insertShardingKeyObj[shardingKey] = shardingKeyObj;
                    }
                    for (let i = 0; i < extendCLNameArray.length; i++) {
                        //logger.debug(JSON.stringify(extendCLRangeArray[i]) + "|" + extendCLNameArray[i]);
                        // 插入扩展表
                        insertArray.push({"shardingKeyObj":insertShardingKeyObj,"prevCLName":secondCLName,"createCLName":extendCLNameArray[i],"upBound":extendCLRangeArray[i].UpBound,"lowBound":extendCLRangeArray[i].LowBound});
                    }
                    //logger.debug(JSON.stringify(insertArray));
                    extendcl.insert(insertArray);
                } catch (error) {
                    logger.except("向表 [" +  EXTENDCLNAME + "] 中插入数据失败", error);
                    throw error;
                }
            }
        } catch (error) {
            logger.except("查询表 [" + DATACS + "." + LASTCLNAME + "] 数据，获取时间间隔失败", error);
            throw error;
        }
    } catch (error) {
        logger.except("获取子表信息失败", error);
        throw error;
    }

    return subCLsize;
}

function getCLInfo() {
    try {
        // 找出匹配的 SNAPSHOT_CL
        try {
            //logger.debug('insert into ' + DATACS + '.' + MATCHCLNAME + ' select b.* from ' + DATACS + '.' + MATCHCATANAME + ' as a inner join ' + DATACS + '.' + SNAPSHOTCLNAME + ' as b on a.Name = b.Name');
            db.execUpdate('insert into ' + DATACS + '.' + MATCHCLNAME + ' select b.Name,b.CollectionSpace,b.Details from ' + DATACS + '.' + MATCHCATANAME + ' as a inner join ' + DATACS + '.' + SNAPSHOTCLNAME + ' as b on a.Name = b.Name');
        } catch (error) {
            logger.except("根据表 [" + DATACS + "." + MATCHCATANAME + "] 中匹配命中的表去 [" + DATACS + "." + SNAPSHOTCLNAME + "] 中查找 SNAPSHOT_CL 信息失败", error);
            throw error;
        }

        // 找出匹配 CS 对应的 domain，这个只能一个个找
        try {
            //logger.debug('select a.Name from ' + DATACS + '.' + SNAPSHOTCSNAME + ' as a inner join ' + DATACS + '.' + MATCHCLNAME + ' as b on a.Name = b.CollectionSpace');
            let cursor = db.exec('select a.Name from ' + DATACS + '.' + SNAPSHOTCSNAME + ' as a inner join ' + DATACS + '.' + MATCHCLNAME + ' as b on a.Name = b.CollectionSpace group by a.Name');
            while (cursor.next()) {
                let csName = cursor.current().toObj().Name;
                let domain;
                try {
                    domain = db.getCS(csName).getDomainName();
                } catch (error) {
                    logger.except("获取 CS [" + csName + "] 的 doaminName 失败", error);
                    throw error;
                }

                try {
                    db.getCS(DATACS).getCL(CSDOAMINNAME).insert({"CSName":csName,"DomainName":domain});
                } catch (error) {
                    logger.except("往表 [" + CSDOAMINNAME + "] 中插入 CS 与 DOMAIN 关系失败", error);
                    throw error;
                }
            }
        } catch (error) {
            logger.except("根据表 [" + DATACS + "." + MATCHCATANAME + "] 中匹配命中的表去 [" + DATACS + "." + SNAPSHOTCSNAME + "] 中查找 CS 信息失败", error);
            throw error;
        }

        try {
            let cursor = db.exec('select t.Details.TotalRecords,t.Details.TotalLobs,t.Details.TotalDataPages, t.Details.TotalIndexPages, t.Details.TotalDataFreeSpace, t.Details.TotalIndexFreeSpace, t.Details.PageSize, \
            t.Details.GroupName,t.Name from (select Details,Name from ' + DATACS + '.' + MATCHCLNAME + ' split by Details) as t');
            let cl = db.getCS(DATACS).getCL(GROUPSIZENAME);
            while (cursor.next()) {
                let current = cursor.current().toObj();
                let insertObj = {};
                let currentGroupSizeGB = ((current.TotalDataPages + current.TotalIndexPages) * current.PageSize - current.TotalDataFreeSpace - current.TotalIndexFreeSpace) / 1024 / 1024 / 1024;
                insertObj['currentGroupSizeGB'] = currentGroupSizeGB;
                insertObj['GroupName'] = current.GroupName;
                insertObj['Name'] = current.Name;
                insertObj['groupTotalRecords'] = current.TotalRecords;
                insertObj['groupTotalLobs'] = current.TotalLobs;
                //logger.debug(JSON.stringify(insertObj));
                cl.insert(insertObj);
            }
            cursor.close();
        } catch (error) {
            logger.except("向表 [" + DATACS + "." + GROUPSIZENAME  + "] 插入数据失败", error);
            throw error;
        }

        try {
            db.execUpdate('insert into ' + DATACS + "." + CLSIZENAME + ' select sum(groupTotalRecords) as totalRecords,sum(groupTotalLobs) as totalLobs,sum(currentGroupSizeGB) as totalSizeGB, max(currentGroupSizeGB) as groupMaxSizeGB, \
            min(currentGroupSizeGB) as groupMinSizeGB, count(GroupName) as groupNum, Name, push(GroupName) as groupArray \
            from ' + DATACS + "." + GROUPSIZENAME + ' group by Name');
        } catch (error) {
            logger.except("对表 [" + DATACS + "." + GROUPSIZENAME  + "] 进行数据聚集运算时失败", error);
            throw error;
        }
    } catch (error) {
        logger.except("无法获取 CL 详细信息", error);
        throw error;
    }

    // 建之前几个表的信息做一个合并到新表中，以便最后输出时不用 inner join, join 字段都是完整的表名 Name
    // 从 MATCH_CATA 中取出 Name,MainCLName,appName
    // 从 SNAPSHOT_CS 中取出 PageSize,LobPageSize
    // 从 CL_SIZE 中取出 totalSizeGB,groupMaxSizeGB,groupMinSizeGB,groupNum,groupArray
    // 从 CS_DOMAIN 中取出 domain

    // 先通过 MATCH_CL 和 SNAPSHOT_CS，CS_DOMAIN 记录 CL 与 CS 中 PageSize,LobPageSize,domain 的关系，插入到 HRBRID_CS_CL 中
    try {
        // logger.debug('insert into ' + DATACS + '.' + HYBRIDCSCLNAME + ' \
        //     select t1.Name,t1.CollectionSpace as CSName,t1.PageSize as pageSize,t1.LobPageSize as lobPageSize,t2.DomainName as domain from (select b.Name,b.CollectionSpace,a.PageSize,a.LobPageSize \
        //     from ' + DATACS + '.' + SNAPSHOTCSNAME + ' \
        //     as a inner join ' + DATACS + '.' + MATCHCLNAME + ' as b \
        //     on a.Name = b.CollectionSpace split by b.Details) as t1 \
        //     inner join ' + DATACS + '.' + CSDOAMINNAME + ' as t2 on t1.CollectionSpace = t2.CSName group by t1.Name');
        db.execUpdate('insert into ' + DATACS + '.' + HYBRIDCSCLNAME + ' \
            select t1.Name,t1.CollectionSpace as CSName,t1.PageSize as pageSize,t1.LobPageSize as lobPageSize,t2.DomainName as domain from (select b.Name,b.CollectionSpace,a.PageSize,a.LobPageSize \
            from ' + DATACS + '.' + SNAPSHOTCSNAME + ' \
            as a inner join ' + DATACS + '.' + MATCHCLNAME + ' as b \
            on a.Name = b.CollectionSpace split by b.Details) as t1 \
            inner join ' + DATACS + '.' + CSDOAMINNAME + ' as t2 on t1.CollectionSpace = t2.CSName group by t1.Name');
    } catch (error) {
        logger.except("无法合并表 [" + DATACS + "." + SNAPSHOTCSNAME + " " + DATACS + "." + MATCHCLNAME + " " + DATACS + "." + CSDOAMINNAME + "] 数据到 [" + DATACS + "." + HYBRIDCSCLNAME + "] 中", error);
        throw error;
    }

    // 然后从 MATCH_CATA 中取出 Name,MainCLName,appName,从 CL_SIZE 中取出 totalSizeGB,groupMaxSizeGB,groupMinSizeGB,groupNum,groupArray，先合并到临时表
    try {
        db.execUpdate('insert into ' + DATACS + "." + TMPCLNAME + ' select a.Name,a.MainCLName,a.appName,b.totalRecords,b.totalLobs,b.totalSizeGB,b.groupMaxSizeGB,b.groupMinSizeGB,b.groupNum,b.groupArray from ' + DATACS + '.' + MATCHCATANAME + ' as a inner join ' + DATACS + '.' + CLSIZENAME + ' as b on a.Name = b.Name')
    } catch (error) {
        logger.except("无法合并表 [" + DATACS + "." + MATCHCATANAME + " " + DATACS + "." + CLSIZENAME + "] 数据到 [" + DATACS + "." + TMPCLNAME + "] 中", error);
        throw error;
    }

    // 然后把最终记录合并到 OUTPUT 表
    try {
        db.execUpdate('insert into ' + DATACS + '.' + OUTOUTCLNAME + ' select a.Name,a.CSName,a.pageSize,a.lobPageSize,a.domain,b.MainCLName,b.appName,b.totalRecords,b.totalLobs,b.totalSizeGB,b.groupMaxSizeGB,b.groupMinSizeGB,b.groupNum,b.groupArray from ' + DATACS + '.' + HYBRIDCSCLNAME + ' as a inner join ' + DATACS + "." + TMPCLNAME + ' as b on a.Name = b.Name')
    } catch (error) {
        logger.except("无法合并表 [" + DATACS + "." + HYBRIDCSCLNAME + " " + DATACS + "." + TMPCLNAME + "] 数据到 [" + DATACS + "." + OUTOUTCLNAME + "] 中", error);
        throw error;
    }

    // 清空临时表
    try {
        db.getCS(DATACS).getCL(TMPCLNAME).truncate();
    } catch (error) {
        logger.except("删除临时表 [" + DATACS + "." + TMPCLNAME + "] 数据失败", error);
        throw error;
    }
}

function outputCSV() {
    let CLCsv;
    let CLTypeCsv;
    let addCLCsv;

    try {
        // 输出表维度 csv: 表名，主表名，应用名，数据域，总记录数，LOB数，总数据量，每个组平均数据量，组上最大数据量，组上最小数据量，最大/最小偏差值，组数，所在数据组
        CLCsv = new File(CLCSV);
        CLCsv.write("CLName,mainCLName,appName,domain,totalRecord,totalLobs,totalSizeGB,groupAvgSizeGB,groupMaxSizeGB,groupMinSizeGB,Max/Min,groupNum,groups" + '\n');
        // 输出表类型维度 csv: 表类型(主表)，应用名，最后一个时间分区增量数据，时间分区平均数据量，时间分区最大数据量，时间分区最少数据量，最大/最小偏差值，总数据量，总数据组，表个数，总分区数
        CLTypeCsv = new File(CLTYPECSV);
        CLTypeCsv.write("type,appName,lastTimeSizeGB,timeAvgSizeGB,timeMaxSizeGB,timeMinSizeGB,timeMax/timeMin,totalSizeGB,totalGroupNum,CLCount,totalPartNum" + '\n');
        // 输出表类型维度 csv: 应用名，主表名，创建表名，表类型，域，组个数，挂载字段，挂载上下界，卸载的 MAX 表，pagesize，logpagesize，partition, 索引使用通用还是继承 ,是否主表
        addCLCsv = new File(ADDCLCSV);
        addCLCsv.write("appName,mainCLName,createCLName,type,domain,groupNum,shardingKey,lowBound,upBound,detachCL,pageSize,lobPageSize,Partition,indexType,isMainCL" + '\n');
    } catch (error) {
        logger.except("文件写入出错", error);
        throw error;
    }

    // 默认MAX表为空，所有数据计算都不包含 MAX 表
    try {
        let cursor = db.exec('select * from ' + DATACS + "." + OUTOUTCLNAME);
        let loop = 0;
        // 输出表维度 information_by_cl.csv
        while(cursor.next()) {
            loop++;
            if (loop % INFOOUTPUTONCE == 0) {
                logger.info("已输出 " + loop + " 条信息");
            }
            let CLCsvLine = [];
            let current = cursor.current().toObj();
            CLCsvLine.push(current.Name);
            CLCsvLine.push(current.MainCLName);
            CLCsvLine.push(current.appName);
            CLCsvLine.push(current.domain);
            CLCsvLine.push(current.totalRecords);
            CLCsvLine.push(current.totalLobs);
            CLCsvLine.push(current.totalSizeGB.toFixed(FIXNUM));
            CLCsvLine.push((current.totalSizeGB/current.groupNum).toFixed(FIXNUM));
            CLCsvLine.push(current.groupMaxSizeGB.toFixed(FIXNUM));
            CLCsvLine.push(current.groupMinSizeGB.toFixed(FIXNUM));
            if ((current.groupMaxSizeGB - current.groupMinSizeGB) * 1024 > MAX_MIN) {
                CLCsvLine.push((current.groupMaxSizeGB/current.groupMinSizeGB).toFixed(FIXNUM));
            } else {
                CLCsvLine.push(0);
            }
            CLCsvLine.push(current.groupNum);
            CLCsvLine.push(current.groupArray);
            CLCsv.write(CLCsvLine.join(',') + "\n");
        }
        cursor.close();

        // 表类型维度，主表
        // cursor = db.exec('select * from ' + DATACS + "." + OUTOUTCLNAME + ' where MainCLName is not null group by MainCLName');
        // while(cursor.next()) {
        //     loop++;
        //     if (loop % 200 == 0) {
        //         logger.info("已输出 " + loop + " 条信息");
        //     }
        //     let CLTypeLine = [];
        //     let current = cursor.current().toObj();
        //     CLTypeLine.push(current.modelType);
        //     CLTypeLine.push(current.appName);
        //     CLTypeLine.push(tmpCursor.current().toObj().totalSizeGB.toFixed(FIXNUM));
        //     CLTypeLine.push((current.totalSizeGB / current.totalPartNum).toFixed(FIXNUM));
        //     CLTypeLine.push(current.timeMaxSizeGB.toFixed(FIXNUM));
        //     CLTypeLine.push(current.timeMinSizeGB.toFixed(FIXNUM));
        //     CLTypeLine.push((current.timeMaxSizeGB / current.timeMinSizeGB).toFixed(FIXNUM));
        //     CLTypeLine.push(current.totalSizeGB.toFixed(FIXNUM));
        //     // groupNum, 加上前面通过 current 取掉的一个
        //     CLTypeLine.push(tmpCursor.size() + 1);
        //     CLTypeLine.push(current.totalPartNum + (current.mainCLName == "" ? 0 : 1) + (current.maxCLName == "" ? 0 : 1));
        //     CLTypeLine.push(current.totalPartNum + (current.maxCLName == "" ? 0 : 1));
        //     CLTypeCsv.write(CLTypeLine.join(',') + "\n");
        //     tmpCursor.close();
        // }
        // cursor.close();

        // 表类型维度，普通表
    

        // // 扩展表维度
        // let extendFullName = DATACS + "." + EXTENDCLNAME;
        // cursor = db.exec('select * from ' + extendFullName);
        // while(cursor.next()) {
        //     loop++;
        //     if (loop % 200 == 0) {
        //         logger.info("已输出 " + loop + " 条信息");
        //     }
        //     let addCLCsvLine = [];
        //     let current = cursor.current().toObj();
        //     let detailCur = db.exec('select appName,domain,groupNum,modelType,mainCLName,maxCLName,shardingKeyObj from ' + DATACS + "." + MATCHCLNAME + ' where isMainCL = false and CLName = "' + current.prevCLName + '"');
        //     let detailCurrent = detailCur.current().toObj();
        //     addCLCsvLine.push(detailCurrent.appName);
        //     addCLCsvLine.push(detailCurrent.mainCLName);
        //     addCLCsvLine.push(current.createCLName);
        //     addCLCsvLine.push(detailCurrent.modelType);
        //     addCLCsvLine.push(detailCurrent.domain);
        //     addCLCsvLine.push(detailCurrent.groupNum);
        //     if (detailCurrent.mainCLName != "") {
        //         let shardingkeyCur = db.exec('select shardingKeyObj from ' + DATACS + "." + MATCHCLNAME + ' where mainCLName = "' + detailCurrent.mainCLName + '" and isMainCL = true');
        //         let shardingKeyObj = shardingkeyCur.current().toObj().shardingKeyObj;
        //         shardingkeyCur.close();
        //         addCLCsvLine.push(Object.keys(shardingKeyObj)[0]);
        //     } else {
        //         addCLCsvLine.push(Object.keys(detailCurrent.shardingKeyObj)[0]);
        //     }
        //     addCLCsvLine.push(current.lowBound);
        //     addCLCsvLine.push(current.upBound);
        //     addCLCsvLine.push(detailCurrent.maxCLName);
        //     addCLCsvLine.push(current.pageSize);
        //     addCLCsvLine.push(current.lobPageSize);
        //     addCLCsvLine.push(current.partition);
        //     addCLCsvLine.push("inherit");
        //     addCLCsvLine.push(false);
        //     addCLCsv.write(addCLCsvLine + "\n");
        //     detailCur.close();
        // }
        // cursor.close();
    } catch (error) {
        logger.except("输出 CSV 文件失败", error);
        throw error;
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
    } catch (error) {
        throw error;
    }

    let detailFullName = DATACS + "." + DETAILINFOCLNAME;
    let modelJson = {};
    let model_cl_array = [];
    let CLObj = {};

    // MAX 表，需要所有主子表都有或没有，如果部分有，部分没有，为 hybrid
    try {
        let emptyMaxCLSize = db.exec('select maxCLName from ' + detailFullName + ' where mainCLName <> "" and maxCLName = ""').size();
        let allMaxCLSize = db.exec('select maxCLName from ' + detailFullName + ' where mainCLName <> ""').size();
        if (emptyMaxCLSize == allMaxCLSize) {
            modelJson["include_max_range"] = false;
        } else if (emptyMaxCLSize == 0) {
            // 此处 allMaxCLSize 不会为 0
            modelJson["include_max_range"] = true;
        } else {
            modelJson["include_max_range"] = "hybrid";
        }
    } catch (error) {
        logger.except("获取模型 MAX 分区是否存在失败", error);
        throw error;
    }

    // 主表
    try {
        let cursor = db.exec('select modelType,findCondition,shardingType,shardingKeyObj from ' + detailFullName + ' where isMainCL = true group by modelType');
        while (cursor.next()) {
            let current = cursor.current().toObj();
            CLObj['model'] = current.findCondition;
            CLObj['type'] = current.modelType;
            let shardingkey = {};
            shardingkey["type"] = current.shardingType;
            shardingkey["key"] = current.shardingKeyObj;
            CLObj['shardingkey'] = shardingkey;
            model_cl_array.push(CLObj);
            CLObj = {};
        }
        modelJson['main_cl'] = model_cl_array;
        model_cl_array = [];
        cursor.close();
    } catch (error) {
        logger.except("获取主表模型信息失败", error);
        throw error;
    }

    // 子表
    try {
        let cursor = db.exec('select modelType,findCondition,shardingType,shardingKeyObj,mainCLName from ' + detailFullName + ' where isMainCL = false and mainCLName <> "" group by modelType');
        while (cursor.next()) {
            let current = cursor.current().toObj();
            let tmpCur = db.exec('select subCLFindCondition from ' + detailFullName + ' where isMainCL = true and mainCLName = "' + current.mainCLName + '"').current().toObj();
            CLObj['model'] = tmpCur.subCLFindCondition;
            CLObj['type'] = current.modelType;
            let shardingkey = {};
            shardingkey["type"] = current.shardingType;
            shardingkey["key"] = current.shardingKeyObj;
            CLObj['shardingkey'] = shardingkey;
            model_cl_array.push(CLObj);
            CLObj = {};
        }
        modelJson['sub_cl'] = model_cl_array;
        model_cl_array = [];
        cursor.close();
    } catch (error) {
        logger.except("获取子表模型信息失败", error);
        throw error;
    }

    // 普通表
    try {
        let cursor = db.exec('select modelType,findCondition,shardingType,shardingKeyObj from ' + detailFullName + ' where isMainCL = false and mainCLName = "" group by modelType');
        while (cursor.next()) {
            let current = cursor.current().toObj();
            CLObj['model'] = current.findCondition;
            CLObj['type'] = current.modelType;
            let shardingkey = {};
            shardingkey["type"] = current.shardingType;
            shardingkey["key"] = current.shardingKeyObj;
            CLObj['shardingkey'] = shardingkey;
            model_cl_array.push(CLObj);
            CLObj = {};
        }
        modelJson['normal_cl'] = model_cl_array;
        model_cl_array = [];
        cursor.close();
    } catch (error) {
        logger.except("获取普通表模型信息失败", error);
        throw error;
    }

    try {
        modelFile.write(JSON.stringify(modelJson, null, 2) + '\n');
        modelFile.close();
    } catch (error) {
        logger.except("输出模型信息到文件 [" + CURRENTMODELJSON + "] 失败", error);
        throw error;
    }
}

//全部不匹配打一个，匹配到一个也打一个
function checkModel() {
    try {
        let cmd = new Cmd();
        let modelFileArray = cmd.run("ls", MODELDIR).split("\n");
        let isFind = false;
        //logger.info(modelFileArray);
        for (let i = 0; i < modelFileArray.length; i++) {
            let fileName = modelFileArray[i];
            if (fileName == "") {continue;}
            let baseModelFile = MODELDIR + fileName;
            //logger.info(baseModelFile);
            let ret = checkOneModel(baseModelFile);
            if (ret || ret == undefined) {
                logger.info("当前模型与标准模型 " + baseModelFile + " 符合");
                isFind = true;
                break;
            }
        }
        if (!isFind) {
            logger.info("在 " + MODELDIR + " 目录下未找到与当前模型符合的模型");
        }
    } catch (error) {
        logger.except("检查模型 [" + CURRENTMODELJSON + "] 失败", error);
        throw error;
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
                // 对比模型
                if (base.model != cur.model) { return false; }
                if (base.shardingkey.type != cur.shardingkey.type) { return false; }
                let baseKey = base.shardingkey.key;
                let curKey = cur.shardingkey.key;
                let isSame = false;
                if (Array.isArray(baseKey)) {
                    //logger.info(baseKey + "is Array");
                    for (let k = 0; k < baseKey.length; k++) {
                        //logger.info("baseKey: " + JSON.stringify(baseKey[i]) + ", curKey: " + JSON.stringify(curKey));
                        if (JSON.stringify(baseKey[i]) == JSON.stringify(curKey)) {
                            isSame = true;
                        }
                    }
                } else if (typeof baseKey === 'object') {
                    //logger.info(baseKey + "is Object");
                    if (JSON.stringify(baseKey) == JSON.stringify(curKey)) {
                        isSame = true;
                    }
                }
                return isSame;
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
        // 对比 subCL
        if (!checkModelCLArray(baseModelObj.sub_cl, curModelObj.sub_cl)) {return false;}
        // 对比 normalCL
        if (!checkModelCLArray(baseModelObj.normal_cl, curModelObj.normal_cl)) {return false;}

        return true;
    } catch (error) {
        logger.except("检查模型 [" + modelFile  + "] 失败", error);
        throw error;
    }
}

function outputFile() {
    removeFile(CLCSV);
    removeFile(CLTYPECSV);
    removeFile(ADDCLCSV);
    outputCSV();
    logger.info("表信息输出完成");
    logger.info("表维度信息文件: " + CLCSV);
    logger.info("表类型维度信息文件: " + CLTYPECSV);
    logger.info("扩展表信息文件: " + ADDCLCSV);
    // outputModel();
    // logger.info("模型信息输出完成");
    // logger.info("模型信息文件: " + CURRENTMODELJSON);
    // checkModel();
    // logger.info("模型匹配完成");
}

function getSnapshot() {
    logger.info("开始获取 $SNAPSHOT_CATA 数据");
    try {
        if (USELOCAL) {
            db.execUpdate('insert into ' + DATACS + "." + SNAPSHOTCATANAME + ' select * from ' + LOCALCATA);
        } else {
            db.execUpdate('insert into ' + DATACS + "." + SNAPSHOTCATANAME + ' select * from $SNAPSHOT_CATA');
        }
    } catch (error) {
        logger.except("查询 $SNAPSHOT_CATA 数据插入到 [" + DATACS + "." + SNAPSHOTCATANAME + "] 失败", error);
        throw error;
    }

    logger.info("获取完成，开始获取主节点 $SNAPSHOT_CL 数据");
    try {
        if (USELOCAL) {
            db.execUpdate('insert into ' + DATACS + "." + SNAPSHOTCATANAME + ' select * from ' + LOCALCL);
        } else {
            db.execUpdate('insert into ' + DATACS + "." + SNAPSHOTCLNAME + ' select * from $SNAPSHOT_CL where nodeselect = "primary"');
        }
    } catch (error) {
        logger.except("查询 $SNAPSHOT_CL 数据插入到 [" + DATACS + "." + SNAPSHOTCLNAME + "] 失败", error);
        throw error;
    }

    logger.info("获取完成，开始获取主节点 $SNAPSHOT_CS 数据");
    try {
        if (USELOCAL) {
            db.execUpdate('insert into ' + DATACS + "." + SNAPSHOTCATANAME + ' select * from ' + LOCALCS);
        } else {
            db.execUpdate('insert into ' + DATACS + "." + SNAPSHOTCSNAME + ' select * from $SNAPSHOT_CS where nodeselect = "primary"');
        }
    } catch (error) {
        logger.except("查询 $SNAPSHOT_CS 数据插入到 [" + DATACS + "." + SNAPSHOTCSNAME + "] 失败", error);
        throw error;
    }
    logger.info("获取完成");
}

function start() {
    // 获取 SNAPSHOT_CATA,CL,CS 到工具创建的临时数据库
    getSnapshot();

    // 获取主表名
    let mainCLSize = findCL(MAIN_CL_ARRAY, "MainSubCL");
    logger.info("解析主表完成，主表数量为：" + mainCLSize);
    
    // 获取普通表名
    let normalCLSize = findCL(NORMAL_CL_ARRAY, "NormalCL");
    logger.info("解析普通表完成，普通表数量为：" + normalCLSize);

    // 获取全量子表
    let fullSubCLSize = getFullSubCLInfo();
    logger.info("获取全部子表完成，数量为：" + fullSubCLSize);

    // 获取 CL 下的其他信息
    logger.info("开始获取表信息");
    getCLInfo();
    // 计算普通表扩展名
    getNormalCLExtendName();
    logger.info("获取表信息完成，开始生成文件");
    // 输出文件
    outputFile();
}

function checkAllKeyWord() {
    checkKeyWord(MAIN_CL_ARRAY);
    checkKeyWord(NORMAL_CL_ARRAY);
    checkKeyWord(DATE_FORMAT_ARRAY);
}

// 检查关键字，如果有前缀相同的关键字，下面的逻辑会误判，目前没有，后面需要注意
function checkKeyWord(checkWordArray) {
    for (let i = 0; i < checkWordArray.length; i++) {
        let dollorCount = 0;
        let checkWord = JSON.stringify(checkWordArray[i]);
        // 获取 $ 次数
        for (let j = 0; j < checkWord.length; j++) {
            if ('$' === checkWord.charAt(j)) {
                dollorCount++;
            }
        }
        //logger.info(checkWord + " " + dollorCount);
        let keyWordCount = 0;
        // 获取关键字次数
        for (let j = 0; j < KEYWORDARRAY.length; j++) {
            let matchArray = checkWord.split(KEYWORDARRAY[j]);
            keyWordCount += matchArray.length - 1;
        }

        //logger.info(dollorCount + " " + keyWordCount);
        if (dollorCount != keyWordCount) {
            let content = "在 " + checkWord + " 中检查到未匹配到关键字的 $ 符号，请检查 conf/args.js 文件中配置";
            logger.error(content);
            throw new Error(content);
        }
    }
}

function initDataCS() {
    if (DATACS == "" || DATACS == undefined) {
        let content = "bin/args.js 文件中 DATACS 的值为空，请检查";
        logger.error(content);
        throw new Error(content);
    }
    // 如果 CS 不存在，则创建，存在则检查里面的 CL 命名是否符合预期（之前运行残留的）
    try {
        let regex = "";
        let cs = db.getCS(DATACS);
        let CLCount = cs.listCollections().size();
        let CLMatchCount = db.list(4,{Name:{"$regex": "^" + DATACS + "\." + EXTENDCLNAME + "_[0-9]{14}$"}}).size();
        // CLMatchCount += db.list(4,{Name:{"$regex": "^" + DATACS + "\." + DETAILINFOCLNAME + "_[0-9]{14}$"}}).size();
        // CLMatchCount += db.list(4,{Name:{"$regex": "^" + DATACS + "\." + SNAPSHOTCATANAME + "_[0-9]{14}$"}}).size();
        // CLMatchCount += db.list(4,{Name:{"$regex": "^" + DATACS + "\." + SNAPSHOTCLNAME + "_[0-9]{14}$"}}).size();
        // CLMatchCount += db.list(4,{Name:{"$regex": "^" + DATACS + "\." + SNAPSHOTCSNAME + "_[0-9]{14}$"}}).size();
        // CLMatchCount += db.list(4,{Name:{"$regex": "^" + DATACS + "\." + GROUPSIZENAME + "$"}}).size();
        // if (CLCount != 0 && CLMatchCount != CLCount) {
        //     let content = "在 CS [" + DATACS + "] 中检查到非此工具创建的 CL，请确认";
        //     logger.error(content);
        //     throw new Error(content);
        // }
    } catch (error) {
        if (error == -34) {
            db.createCS(DATACS);
        } else {
            logger.except("检查 DATACS [" + DATACS + "] 失败", error);
            throw error;
        }
    }

    // 创建当前时间的 CL
    let dateStr = date2Str(new Date(), true);
    //dateStr = "";
    let CLNameArray = [];
    SNAPSHOTCATANAME = SNAPSHOTCATANAME + "_" + dateStr;
    try {
        db.getCS(DATACS).createCL(SNAPSHOTCATANAME);
        CLNameArray.push(SNAPSHOTCATANAME);
    } catch (error) {
        logger.except("在 DATACS [" + DATACS + "] 下创建 [" + SNAPSHOTCATANAME + "] 失败", error);
        throw error;
    }

    SNAPSHOTCLNAME = SNAPSHOTCLNAME + "_" + dateStr;
    try {
        db.getCS(DATACS).createCL(SNAPSHOTCLNAME);
        CLNameArray.push(SNAPSHOTCLNAME);
    } catch (error) {
        logger.except("在 DATACS [" + DATACS + "] 下创建 [" + SNAPSHOTCLNAME + "] 失败", error);
        throw error;
    }

    SNAPSHOTCSNAME = SNAPSHOTCSNAME + "_" + dateStr;
    try {
        db.getCS(DATACS).createCL(SNAPSHOTCSNAME);
        CLNameArray.push(SNAPSHOTCSNAME);
    } catch (error) {
        logger.except("在 DATACS [" + DATACS + "] 下创建 [" + SNAPSHOTCSNAME + "] 失败", error);
        throw error;
    }

    try {
        db.getCS(DATACS).createCL(EXTENDCLNAME);
        CLNameArray.push(EXTENDCLNAME);
    } catch (error) {
        if (error == -22) {
            db.getCS(DATACS).getCL(EXTENDCLNAME).truncate();
        } else {
            logger.except("在 DATACS [" + DATACS + "] 下创建 [" + EXTENDCLNAME + "] 失败", error);
            throw error;
        }
    }

    try {
        db.getCS(DATACS).createCL(OUTOUTCLNAME);
        CLNameArray.push(OUTOUTCLNAME);
    } catch (error) {
        if (error == -22) {
            db.getCS(DATACS).getCL(OUTOUTCLNAME).truncate();
        } else {
            logger.except("在 DATACS [" + DATACS + "] 下创建 [" + OUTOUTCLNAME + "] 失败", error);
            throw error;
        }
    }

    try {
        db.getCS(DATACS).createCL(GROUPSIZENAME);
        CLNameArray.push(GROUPSIZENAME);
    } catch (error) {
        if (error == -22) {
            db.getCS(DATACS).getCL(GROUPSIZENAME).truncate();
        } else {
            logger.except("在 DATACS [" + DATACS + "] 下创建 [" + GROUPSIZENAME + "] 失败", error);
            throw error;
        }
    }

    try {
        db.getCS(DATACS).createCL(CSDOAMINNAME);
        db.getCS(DATACS).getCL(CSDOAMINNAME).createIndex("sort",{"value":-1})
        CLNameArray.push(CSDOAMINNAME);
    } catch (error) {
        if (error == -22) {
            db.getCS(DATACS).getCL(CSDOAMINNAME).truncate();
        } else {
            logger.except("在 DATACS [" + DATACS + "] 下创建 [" + CSDOAMINNAME + "] 失败", error);
            throw error;
        }
    }

    try {
        db.getCS(DATACS).createCL(SUBCLFINDCONDCLNAME);
        db.getCS(DATACS).getCL(SUBCLFINDCONDCLNAME).createIndex("sort",{"value":-1})
        CLNameArray.push(SUBCLFINDCONDCLNAME);
    } catch (error) {
        if (error == -22) {
            db.getCS(DATACS).getCL(SUBCLFINDCONDCLNAME).truncate();
        } else {
            logger.except("在 DATACS [" + DATACS + "] 下创建 [" + SUBCLFINDCONDCLNAME + "] 失败", error);
            throw error;
        }
    }

    try {
        db.getCS(DATACS).createCL(TMPCLNAME);
        db.getCS(DATACS).getCL(TMPCLNAME).createIndex("sort",{"value":-1})
        CLNameArray.push(TMPCLNAME);
    } catch (error) {
        if (error == -22) {
            db.getCS(DATACS).getCL(TMPCLNAME).truncate();
        } else {
            logger.except("在 DATACS [" + DATACS + "] 下创建 [" + TMPCLNAME + "] 失败", error);
            throw error;
        }
    }

    try {
        db.getCS(DATACS).createCL(CLSIZENAME);
        CLNameArray.push(CLSIZENAME);
    } catch (error) {
        if (error == -22) {
            db.getCS(DATACS).getCL(CLSIZENAME).truncate();
        } else {
            logger.except("在 DATACS [" + DATACS + "] 下创建 [" + CLSIZENAME + "] 失败", error);
            throw error;
        }
    }

    try {
        db.getCS(DATACS).createCL(MATCHCLNAME);
        CLNameArray.push(MATCHCLNAME);
    } catch (error) {
        if (error == -22) {
            db.getCS(DATACS).getCL(MATCHCLNAME).truncate();
        } else {
            logger.except("在 DATACS [" + DATACS + "] 下创建 [" + MATCHCLNAME + "] 失败", error);
            throw error;
        }
    }

    try {
        db.getCS(DATACS).createCL(MATCHCATANAME);
        CLNameArray.push(MATCHCATANAME);
    } catch (error) {
        if (error == -22) {
            db.getCS(DATACS).getCL(MATCHCATANAME).truncate();
        } else {
            logger.except("在 DATACS [" + DATACS + "] 下创建 [" + MATCHCATANAME + "] 失败", error);
            throw error;
        }
    }

    try {
        db.getCS(DATACS).createCL(HYBRIDCSCLNAME);
        CLNameArray.push(HYBRIDCSCLNAME);
    } catch (error) {
        if (error == -22) {
            db.getCS(DATACS).getCL(HYBRIDCSCLNAME).truncate();
        } else {
            logger.except("在 DATACS [" + DATACS + "] 下创建 [" + HYBRIDCSCLNAME + "] 失败", error);
            throw error;
        }
    }
    
    try {
        db.getCS(DATACS).createCL(MAINMAXNAME);
        CLNameArray.push(MAINMAXNAME);
    } catch (error) {
        if (error == -22) {
            db.getCS(DATACS).getCL(MAINMAXNAME).truncate();
        } else {
            logger.except("在 DATACS [" + DATACS + "] 下创建 [" + MAINMAXNAME + "] 失败", error);
            throw error;
        }
    }

    try {
        db.getCS(DATACS).createCL(LASTCLNAME);
        CLNameArray.push(LASTCLNAME);
    } catch (error) {
        if (error == -22) {
            db.getCS(DATACS).getCL(LASTCLNAME).truncate();
        } else {
            logger.except("在 DATACS [" + DATACS + "] 下创建 [" + LASTCLNAME + "] 失败", error);
            throw error;
        }
    }

    return CLNameArray;
}

function removeDataCL(CLNameArray) {
    if (DROPDATACS == true) {
        for (let i = 0; i < CLNameArray.length; i++) {
            try {
                db.getCS(DATACS).dropCL(CLNameArray[i]);
            } catch (error) {
                logger.except("在 DATACS [" + DATACS + "] 下删除 [" + CLNameArray[i] + "] 失败", error);
                throw error;
            }
        }

        try {
            //db.dropCS(DATACS,{EnsureEmpty:true});
            db.dropCS(DATACS);
        } catch (error) {
            logger.except("删除 DATACS [" + DATACS + "] 失败", error);
            throw error;
        }
    }
}

/*
    start
*/

function main() {
    checkAllKeyWord();
    let CLNameArray = initDataCS();
    start();
    // 测试使用
    removeDataCL(CLNameArray);
}

main();