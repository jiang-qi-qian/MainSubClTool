// 收集快照数据为 collect, 执行分析为 run，本工具用于检查，没有回滚功能
if (typeof MODE == "undefined" || MODE == null || MODE == "") {
    var MODE = "collect";
}

// ------------ 外部参数，根据实际情况修改 ------------
// 找最后一张表时是否跳过空表，默认为 true
var SKIPEMPTYBYLASTCL = true;

// 每收集/输出多少条记录，打印一次信息，默认值为 10000
var INFOOUTPUTONCE = 10000;

// 计算数值后保留多少位小数，默认值为 4
var FIXNUM = 4;

// MAX - MIN 差大于此值才会计算 MAX/MIN，单位为 M，默认值为 100
var MAX_MIN = 100;

// ------------ 外部参数，大部分情况下不需要修改 ------------
// 收集快照信息文件路径，在 collect 模式中为写入，在 run 模式中为读取
var SNAPSHOTCATAFILE = "output/snapshot_cata.out";
var SNAPSHOTCLFILE = "output/snapshot_cl.out";
var SNAPSHOTCSFILE = "output/snapshot_cs.out";
var LISTCSFILE = "output/list_cs.out";
var SNAPSHOTSYSTEMFILE = "output/snapshot_system.out";

SNAPSHOTCATAFILE = "/data/code/cl_tool/shanghai/20240403_inspect/snapshotcata.txt";
SNAPSHOTCLFILE = "/data/code/cl_tool/shanghai/20240403_inspect/snapshotcl.txt";
LISTCSFILE = "/data/code/cl_tool/shanghai/20240403_inspect/listcs.txt";

// 读取的文件路径
var CONFIGJSON = "conf/config.json";
var MODELDIR = "conf/model/";

// 输出的文件路径
var CLCSV = "output/information_by_cl.csv";
var CLTYPECSV = "output/information_by_cl_type.csv";
var CURRENTMODELJSON = "output/current_model.json";
var ADDCLCSV = "output/add_cl.csv";

// 用于存储原始的快照信息
var SNAPSHOTCATANAME = "SNAPSHOT_CATA";
var SNAPSHOTCLNAME = "SNAPSHOT_CL";
var SNAPSHOTCSNAME = "SNAPSHOT_CS";
var LISTCSNAME = "LIST_CS";

// 中途临时存储数据的 CL 名。最后会删除
// 存储从 SNAPSHOT_CATA 正则匹配命中的 CL 名的 CATA 信息
var MATCHCATANAME = "MATCH_CATA"
// 存储从 SNAPSHOT_CL 匹配命中的 CL 名的 CL 信息
var MATCHCLNAME = "MATCH_CL"
// 混合 LIST_CS 和 SNAPSHOT_CL 配置的表
var HYBRIDCSCLNAME = "HYBRID_CS_CL"
// CL 每个组的数据量大小
var GROUPSIZENAME = "GROUP_SIZE";
// CL 级数据量大小，包括 MAX MIN 表
var CLSIZENAME = "CL_SIZE";
// 记录了表与最后一张表关系
var LASTCLNAME = "LAST_CL";
// 临时表，用于各种临时数据存储，一个地方用完需要马上清除数据
var TMPCLNAME = "TMP";
// 存储主表名和对应子表的查找条件
var SUBCLFINDCONDCLNAME = "SUB_CL_FIND_COND"; 
// 记录了主表与MAX表的关系
var MAINMAXNAME = "MAIN_MAX"
// 扩展表信息
var EXTENDCLNAME = "EXTEND_CL";
// 最终输出的表
var OUTOUTCLNAME = "OUTPUT";
// 上面表的数组，用于对非空的 DATACS 做检查
var TOOLCLARRAY = [SNAPSHOTCATANAME,SNAPSHOTCLNAME,SNAPSHOTCSNAME,LISTCSNAME,MATCHCATANAME,MATCHCLNAME,HYBRIDCSCLNAME,GROUPSIZENAME,CLSIZENAME,LASTCLNAME,TMPCLNAME,SUBCLFINDCONDCLNAME,MAINMAXNAME,EXTENDCLNAME,OUTOUTCLNAME];

// 日期长度，用于把主表 range 切分键的时间字符串转换为时间，默认兼容 20240101 和 20240101000000 两种格式
var DATEFORMAT = { year: 4, month: 6, day: 8, hour: 10, minute: 12, second: 14};

// ------------ 内部变量，不要修改 ------------
var TOOL = "check_model";
// 最后一张子表表名: 扩展表数量
var EXTENDCLCOUNT = {};
var INCLUDEMAXRANGE = false;
importOnce("./args.js");
importOnce("./general.js");
// 排序，把长的条件放在前面
DATE_FORMAT_ARRAY.sort(function(a,b){return a.length < b.length});

function changDate(str) {
    let retArray = [];
    if (str.search("\\$DATE") == -1) {
        let dateStr = str.replace(new RegExp("\\$YYYY", "ig"), "[1-2]\\d{3}");
        dateStr = dateStr.replace(new RegExp("\\$MM", "ig"), "[0-1][0-9]");
        dateStr = dateStr.replace(new RegExp("\\$dd", "ig"), "[0-3][0-9]");
        retArray.push(dateStr);
    } else {
        for (let i = 0; i < DATE_FORMAT_ARRAY.length; i++) {
            let dateStr = DATE_FORMAT_ARRAY[i].replace(new RegExp("\\$YYYY", "i"), "[1-2]\\d{3}");
            dateStr = dateStr.replace("$MM", "[0-1][0-9]");
            dateStr = dateStr.replace(new RegExp("\\$dd", "i"), "[0-3][0-9]");
            // 不支持 时分秒
            // dateStr = dateStr.replace(new RegExp("HH", "i"), "[0-2][0-9]");
            // dateStr = dateStr.replace("mm", "[0-5][0-9]");
            // dateStr = dateStr.replace(new RegExp("ss", "i"), "[0-5][0-9]");
            retArray.push(str.replace(new RegExp("\\$DATE", "g"), dateStr));
        }
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
        logger.info('正在解析 ' + CLtype + ' 表，条件为: ' + CLNameObj[CLtype]);
        let cs = nameArray[0];
        let cl = nameArray[1];
        let csArray = cs.split('$APPNAME');
        let clArray = cl.split('$APPNAME');
        if (csArray.length > 2 || clArray.length > 2 ) {
            let content = "bin/args.js 文件中 [" + JSON.stringify(CLNameObj) + "] 格式有问题，目前 CS 或 CL 中只能有一个 $APPNAME";
            logger.error(content);
            throw new Error(content);
        }

        let cs_prefix;
        let cs_suffix;
        let cl_prefix;
        let cl_suffix;
        let regexStr = "";

        if (csArray.length == 2 && clArray.length == 2) {
            cs_prefix = csArray[0];
            cs_suffix = csArray[1];
            cl_prefix = clArray[0];
            cl_suffix = clArray[1];
            // '^(?!.*LOB)([^.]*).\\1$' 避免匹配 $APPNAME.$APPNAME 时错误匹配 $APPNAME_LOB_XXXX.$APPNAME_LOB_XXXX
            //regexStr = "^(?!.*LOB)" + cs_prefix + "([^\.]*)" + cs_suffix + "\." + cl_prefix + "\\1" + cl_suffix + "$"
            regexStr = "^" + cs_prefix + "([^\.]*)" + cs_suffix + "\." + cl_prefix + "\\1" + cl_suffix + "$"
        } else if (csArray.length == 1 && clArray.length == 2) {
            cs_prefix = csArray[0];
            cs_suffix = "";
            cl_prefix = clArray[0];
            cl_suffix = clArray[1];
            regexStr = "^" + cs_prefix + cs_suffix + "\." + cl_prefix + "(.*?)" + cl_suffix + "$"
        } else if (csArray.length == 2 && clArray.length == 1) {
            cs_prefix = csArray[0];
            cs_suffix = csArray[1];
            cl_prefix = clArray[0];
            cl_suffix = "";
            regexStr = "^" + cs_prefix + "([^\.]*)" + cs_suffix + "\." + cl_prefix + cl_suffix + "$"
        } else {
            let content = "bin/args.js 文件中 [" + JSON.stringify(CLNameObj) + "] 格式有问题，未找到 $APPNAME 关键字";
            logger.error(content);
            throw new Error(content);
        }
        
        //logger.debug(regexStr);
        let condArray = changDate(regexStr);
        // 先获取每个条件的表，并保存
        // 如果不同的条件匹配到相同的表，说明一个更大的匹配条件包括了一个小的匹配条件，那么总匹配表数少的条件会是匹配范围更小的，所以这张表会认为是总匹配表数少的
        let snapshotCataCL = db.getCS(DATACS).getCL(SNAPSHOTCATANAME);
        let matchCL = db.getCS(DATACS).getCL(MATCHCATANAME);
        for (let j = 0; j < condArray.length; j++) {
            //logger.debug(condArray[j]);
            let cursor;
            try {
                cursor = snapshotCataCL.find({Name: {"$regex": condArray[j]}});
            } catch (error) {
                logger.except("查询 " + DATACS + "." + SNAPSHOTCATANAME + ".find(4,{Name:{\"$regex\":\"" + condArray[j] + "\"}}.{Name:1}}) 失败", error);
                throw error;
            }
    
            let size = 0;
            try {
                while (cursor.next()) {
                    size++;
                    let current;
                    try {
                        current = cursor.current().toObj();
                    } catch (error) {
                        logger.except("获取表 [" + DATACS + "." + SNAPSHOTCATANAME + "] 中数据失败", error);
                    }
                    current['appName'] = current['Name'].replace(new RegExp(condArray[j]),"$1");
                    current['findCondition'] = CLNameObj[CLtype];
                    current['modelType'] = CLtype;
                    delete current._id;
                    try {
                        matchCL.insert(current);
                    } catch (error) {
                        logger.except("往表 [" + DATACS + "." + MATCHCATANAME + "] 中插入数据失败", error);
                    }
                }
            } catch (error) {
                throw error;
            } finally {
                cursor.close();
            }
            //logger.debug(condArray[j] + " " + size);
            sizeObj[condArray[j]] = size;
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
            cursor = db.exec('select t1.Name,t1.findCondition as cond1,t2.findCondition as cond2 from ' + DATACS + "." + MATCHCATANAME + ' as t1 inner join ' + DATACS + "." + MATCHCATANAME + ' as t2 on t1.Name = t2.Name where t1.findCondition <> t2.findCondition group by t1.Name /*+use_hash()*/');
        } catch (error) {
            logger.except("查找是否有不同的条件匹配到相同的表失败", error);
            throw error;
        }
        try {
            while(cursor.next()) {
                execTime++;
                let current;
                try {
                    current = cursor.current().toObj();
                } catch (error) {
                    logger.except("获取表 [" + DATACS + "." + MATCHCATANAME + "] 中数据失败", error);
                    throw error;
                }
                try {
                    // 优先匹配少的
                    if (sizeObj[current.cond1] > sizeObj[current.cond2]) {
                        //logger.debug(current.Name + " 1 " + current.cond1);
                        matchCL.remove({$and:[{"Name":{$et:current.Name}},{"findCondition":{$et:current.cond1}}]});
                    } else if (sizeObj[current.cond1] > sizeObj[current.cond2]) {
                        //logger.debug(current.Name + " 2 " + current.cond1);
                        matchCL.remove({$and:[{"Name":{$et:current.Name}},{"findCondition":{$et:current.cond1}}]});
                    // 如果匹配数一样（应该不可能），选长的
                    } else if (sizeObj[current.cond1] == sizeObj[current.cond2] && current.cond1.length < current.cond2.length) {
                        //logger.debug(current.Name + " 3 " + current.cond1);
                        matchCL.remove({$and:[{"Name":{$et:current.Name}},{"findCondition":{$et:current.cond1}}]});
                    // 选第二个(短)
                    } else {
                        //logger.debug(current.Name + " 4 " + current.cond2);
                        matchCL.remove({$and:[{"Name":{$et:current.Name}},{"findCondition":{$et:current.cond2}}]});
                    }
                } catch (error) {
                    logger.except("删除表 [" + DATACS + "." + MATCHCATANAME + "] 中重复匹配数据失败", error);
                    throw error;
                }
            }
        } catch (error) {
            throw error;
        } finally {
            cursor.close();
        }
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
        logger.except("从表 [" + DATACS + "." + MATCHCATANAME + "] 中获取命中的表个数失败", error);
        throw error;
    }
    return retSize;
}

// to YYYY or YYYYMM or YYYYMMdd or YYYYMMddHHmmss
function date2Str(date, length) {
    let dateStr = '' + date.getFullYear();
    if (length >= 6) {
        if (date.getMonth() < 9) {
            dateStr += ('0' + (date.getMonth() + 1));
        } else {
            dateStr += (date.getMonth() + 1);
        }
    }

    if (length >= 8) {
        if (date.getDate() < 10) {
            dateStr += ('0' + date.getDate());
        } else {
            dateStr += date.getDate();
        }
    }

    if (length >= 14) {
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
        month = parseInt(timeStr.substring(DATEFORMAT.year, DATEFORMAT.month), 10) - 1;
    }
    if (timeStr.length == DATEFORMAT.day) {
        month = parseInt(timeStr.substring(DATEFORMAT.year, DATEFORMAT.month), 10) - 1;
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

    try {
        // 根据时间格式字符串查找，逻辑是最后一个分区（除 MAX）有当前时间的年、月、日（由最小精度决定）
        for (let i = 0; i < DATE_FORMAT_ARRAY.length; i++){
            let dateFormat = DATE_FORMAT_ARRAY[i];
            let csName = lastSubCLName.split('.')[0];
            let clName = lastSubCLName.split('.')[1];

            // 通过分区时间差倒推名字，如果分区时间差一年，那么只关心 20240101 中的 2024 ，其余不关心，但目前无法处理 240101 这种的（应该不会有客户用，有歧义）
            // 考虑提前建了后面分区的情况，即传入的子表名为 2025，但当前为 2024
            let newDate = new Date(curDate);
            if (STARTYEAR != 0) {
                newDate.setFullYear(STARTYEAR);
            }
            let CLCount = 0;
            let isFindSubCLCond = false;
            //logger.debug(lastSubCLName + " " + newDate + " " + endTimeDate + " " + lastUpBoundDate);
            while (newDate.getTime() <= endTimeDate.getTime() || newDate.getTime() < lastUpBoundDate.getTime()) {
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
                //logger.debug(lastSubCLName + " " + oldDateStr + " " + newDateStr);
                if ((-1 != csName.search(oldDateStr) || -1 != clName.search(oldDateStr)) && oldDateStr != newDateStr) {
                    //logger.debug(lastSubCLName + " " + oldDateStr + " " + newDateStr);
                    csName = csName.replace(oldDateStr, newDateStr);
                    clName = clName.replace(oldDateStr, newDateStr);
                    //logger.debug(EXTENDCLCOUNT[lastSubCLName]);
                    if (CLCount < EXTENDCLCOUNT[lastSubCLName] || lastUpBound == "") {
                        extendCLNameArray.push(csName + '.' + clName);
                    }

                    //logger.debug("扩展子表名: " + csName + '.' + clName + " " + CLCount);
                    // 获取模型中时间格式
                    let modelDateFormat = "$YYYY";
                    if (timeIntervalObj.diffMonth) {
                        modelDateFormat += "$MM";
                    }
                    if (timeIntervalObj.diffDay) {
                        modelDateFormat += "$dd";
                    }
                    
                    if (mainCL != "" && !isFindSubCLCond) {
                        // 一个表只需要找一次条件
                        isFindSubCLCond = true;
                        try {
                            // 对 $APPNAME 的替换后移到输出时，这里拿 $APPNAME 不方便
                            let findCondition = lastSubCLName.replace(new RegExp(oldDateStr, 'g'), modelDateFormat);
                            //logger.debug("timeIntervalObj " + JSON.stringify(timeIntervalObj) + " lastSubCLName " + lastSubCLName + " mainCLName " + mainCL + " oldDateStr " + oldDateStr + " subCLFindCondtiton " + findCondition + " modelDateFormat " + modelDateFormat);
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
    } catch (error) {
        throw error;
    }
    return extendCLNameArray;
}

function getExtendCLRangeArray(lastSubCLName, firstLowBoundStr, timeIntervalObj) {
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
    let upBoundStr = date2Str(addTimeIntervalObj(lowBoundDate, timeIntervalObj),lowBoundStr.length);
    let loop = 0;
    //logger.debug(lowBoundDate + " " + endTimeDate);
    while (lowBoundDate.getTime() <= endTimeDate.getTime()) {
        //logger.debug("loop " + lowBoundStr + " " + upBoundStr);
        extendCLRangeArray.push({"LowBound":lowBoundStr, "UpBound":upBoundStr});
        lowBoundStr = upBoundStr;
        lowBoundDate = parseDateTime(upBoundStr);
        upBoundStr = date2Str(addTimeIntervalObj(lowBoundDate, timeIntervalObj),lowBoundStr.length);
        loop++;
    }

    EXTENDCLCOUNT[lastSubCLName] = loop;
    return extendCLRangeArray;
}

function getNormalCLExtendName() {
    // 同一应用下同一类型表
    let endTimeDate = parseDateTime(ENDTIME);
    let cursor;

    // 使用 TMPCLNAME 对进行排序查找最后一个表，先把同一模型、同一应用的表放进去
    try {
        db.execUpdate('insert into ' + DATACS + "." + TMPCLNAME + ' select Name as value,appName,modelType from ' + DATACS + "." + MATCHCATANAME + ' where IsMainCL is null and MainCLName is null');
    } catch (error) {
        logger.except("查询表 [" + DATACS + "." + MATCHCATANAME + "] 数据插入到 [" + DATACS + "." + TMPCLNAME + "] 失败", error);
        throw error;
    }

    // 聚集后的 $first 不受 order by 影响，但是 $max 和 $min 可以取到，同时找出倒数第一和第二张表，可能会找不出
    try {
        db.execUpdate('insert into ' + DATACS + "." + LASTCLNAME + ' select a.lastCLName,max(b.value) as secondCLName,b.appName,b.modelType from (select max(value) as lastCLName,appName,modelType from ' + DATACS + "." + TMPCLNAME + ' group by appName,modelType) as a inner join ' + DATACS + "." + TMPCLNAME + ' as b on a.appName = b.appName and a.modelType = b.modelType where a.lastCLName <> b.value group by b.appName,b.modelType /*+use_hash()*/')
    } catch (error) {
        logger.except("查询表 [" + DATACS + "." + TMPCLNAME + "] 数据插入到 [" + DATACS + "." + LASTCLNAME + "] 失败", error);
        throw error;
    }

    try {
        cursor = db.exec('select * from ' + DATACS + "." + LASTCLNAME + ' where mainCLName is null');
    } catch (error) {
        logger.except("获取表 [" + DATACS + "." + LASTCLNAME + "] 中数据失败", error);
        throw error;
    }

    try {
        while (cursor.next()) {
            let timeIntervalObj;
            let current;
            try {
                current = cursor.current().toObj();
            } catch (error) {
                logger.except("获取表 [" + DATACS + "." + LASTCLNAME + "] 中数据失败", error);
                throw error;
            };
            // 解析名字中的时间
            let secondCLName = current.secondCLName.split('.')[1];
            let lastCLName = current.lastCLName;
            let lastCLNameCL = lastCLName.split('.')[1];
            let isFind = false;
            for (let i = 0; i < DATE_FORMAT_ARRAY.length; i++){
                let dateFormat = DATE_FORMAT_ARRAY[i];
                // 匹配格式，只考虑 CL，不考虑 CS
                let lastDate = new Date();
                if (STARTYEAR != 0) {
                    lastDate.setFullYear(STARTYEAR);
                }
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
                    //logger.debug(lastCLNameCL + " " + dateStr + " " + dateFormat);
                    if (-1 != index) {
                        // 最新分区时间
                        let dateStr1 = dateStr;
                        // 前一个分区时间，要求除时间外，格式需要一致
                        let dateStr2 = secondCLName.substring(index, index + dateStr1.length);
                        //logger.debug(lastCLNameCL + " " + dateStr + " " + dateFormat);
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
            if (timeIntervalObj == undefined || (timeIntervalObj.diffYear == 0 && timeIntervalObj.diffMonth == 0 && timeIntervalObj.diffDay == 0)) {
                let content = "无法获取普通表 [" + lastCLName + "] 的时间间隔: " + JSON.stringify(timeIntervalObj) + "，请确认是否需要调整参数 STARTYEAR 的值 " + STARTYEAR + " 以向更早的时间段查找";
                logger.error(content);
                throw new Error(content);
            }
            //logger.debug(lastCLName + " " + JSON.stringify(timeIntervalObj));
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
                    //logger.debug(JSON.stringify(insertArray));
                    extendcl.insert(insertArray);
                } catch (error) {
                    logger.except("向表 [" +  EXTENDCLNAME + "] 中插入数据失败", error);
                    throw error;
                }
            }
        }
    } catch (error) {
        throw error;
    } finally {
        cursor.close();
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
    let cursor;
    try {
        cursor = db.exec('select Name,CataInfo,ShardingKey,ShardingType,appName,modelType from ' + DATACS + '.' + MATCHCATANAME + ' where IsMainCL = true');
    } catch (error) {
        logger.except("获取表 [" + DATACS + "." + MATCHCATANAME + "] 中数据失败", error);
        throw error;
    }
    try {
        while (cursor.next()) {
            let current;
            try {
                current = cursor.current().toObj();
            } catch (error) {
                logger.except("获取表 [" + DATACS + "." + MATCHCATANAME + "] 中数据失败", error);
                throw error;
            }
            let mainCL = current.Name;
            let CataInfo = current.CataInfo;
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
                    db.execUpdate('insert into ' + DATACS + '.' + MATCHCATANAME + ' select a.Name,a.MainCLName,a.CataInfo,a.Partition,a.ShardingKey,a.ShardingType,b.appName,b.modelType from (select Name,CataInfo,ShardingKey,ShardingType,MainCLName,Partition from ' + DATACS + '.' + SNAPSHOTCATANAME + ' where Name = "' + subCLName + '") as a inner join ' + DATACS + '.' + MATCHCATANAME + ' as b on a.MainCLName = b.Name /*+use_hash()*/')
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
                            mainmaxcl.insert({MainCLName:mainCL,maxCLName:subCLName});
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
                        sortcl.insert({"CLName":subCLName,"shardingKey":ShardingKey,"shardingKeyObj":JSON.stringify(current.ShardingKey),"value":upBound,"lowBound":lowBound,"mainCLName":mainCL,"appName":current.appName,"modelType":current.modelType});
                    } catch (error) {
                        logger.except("往临时表 [" + DATACS + "." + TMPCLNAME + "] 中插入数据失败", error);
                        throw error;
                    }
                }
            }

            // 通过临时表找到当前主表的最后一个分区的，并插入到 lastcl 表中
            try {
                db.execUpdate('insert into ' + DATACS + "." + LASTCLNAME + ' \
                select t1.CLName as lastCLName,t1.upBound,t1.lowBound,t2.CLName as secondCLName,t2.mainCLName,t2.shardingKey,t2.appName,t2.modelType from \
                (select b.CLName,a.upBound,b.lowBound from (select max(value) as upBound from ' + DATACS + "." + TMPCLNAME + ' ) \as a inner join ' + DATACS + "." + TMPCLNAME + ' as b on a.upBound = b.value /*+use_hash()*/) \
                as t1 inner join ' + DATACS + "." + TMPCLNAME + ' as t2 on t1.lowBound = t2.value /*+use_hash()*/')
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
    } catch (error) {
        throw error;
    } finally {
        cursor.close();
    }

    try {
        // 查找最后一个分区表
        cursor = db.exec('select lastCLName,shardingKey,shardingKeyObj,lowBound,upBound,mainCLName from ' + DATACS + "." + LASTCLNAME + ' where mainCLName is not null');
    } catch (error) {
        logger.except("获取表 [" + DATACS + "." + LASTCLNAME + "] 中数据失败", error);
        throw error;
    }
    try {
        let timeIntervalObj;
        let extendCLNameArray = [];
        let extendCLRangeArray = [];
        while (cursor.next()) {
            let current;
            try {
                current = cursor.current().toObj();
            } catch (error) {
                logger.except("获取表 [" + DATACS + "." + LASTCLNAME + "] 中数据失败", error);
            }
            let upBound = current.upBound;
            let lowBound = current.lowBound;
            let lastCLName = current.lastCLName;
            let mainCLName = current.mainCLName;
            let shardingKeyObj = current.shardingKeyObj;
            //logger.debug(shardingKeyObj);
            let shardingKey = current.shardingKey;
            // 获取时间间隔
            timeIntervalObj = getTimeIntervalObj(upBound, lowBound);
            if (timeIntervalObj == undefined || timeIntervalObj == "") {
                let content = "通过 UpBound: " + upBound + " 和 LowBound: " + lowBound + " 获取的时间间隔有误: " + JSON.stringify(timeIntervalObj);
                logger.error(content);
                throw new Error(content);
            }
            // 获取时间间隔数组
            //logger.debug(mainCLName + " " + lastCLName + " " + lowBound + " " + upBound + " " + JSON.stringify(timeIntervalObj));
            extendCLRangeArray = getExtendCLRangeArray(lastCLName, upBound, timeIntervalObj);
            // 获取扩展分区的名称数组
            extendCLNameArray = getExtendCLNameArray(mainCLName, lastCLName, upBound, timeIntervalObj);
            //logger.debug(JSON.stringify(extendCLRangeArray));
            //logger.debug(JSON.stringify(extendCLNameArray));
            if (extendCLNameArray.length != extendCLRangeArray.length) {
                let content = "解析出主表 [" + mainCLName + "] 的扩展子表名与扩展上下界不一致，请确认是否需要调整参数 STARTYEAR 的值 " + STARTYEAR + " 以向更早的时间段查找";
                logger.error(content);
                logger.error("子表数量为: " + extendCLNameArray.length);
                for (let j = 0; j < extendCLNameArray.length; j++) {
                    logger.error("子表名：" + extendCLNameArray[j]);
                }
                logger.error("扩展上下界数量为: " + extendCLRangeArray.length);
                for (let j = 0; j < extendCLRangeArray.length; j++) {
                    logger.error("扩展上下界：" + JSON.stringify(extendCLRangeArray[j]));
                }
                throw new Error(content);
            }

            try {
                let insertArray = [];
                let extendcl = db.getCS(DATACS).getCL(EXTENDCLNAME);
                let insertShardingKeyObj = {};
                // 有些情况可能无法处理
                if (lowBound.length == 4) {
                    insertShardingKeyObj[shardingKey] = "$YYYY";
                } else if (lowBound.length == 6) {
                    insertShardingKeyObj[shardingKey] = "$YYYY$MM";
                } else if (lowBound.length == 8) {
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
                    insertArray.push({"mainCLName":mainCLName,"shardingKeyObj":insertShardingKeyObj,"prevCLName":lastCLName,"createCLName":extendCLNameArray[i],"upBound":extendCLRangeArray[i].UpBound,"lowBound":extendCLRangeArray[i].LowBound});
                }
                //logger.debug(JSON.stringify(insertArray));
                extendcl.insert(insertArray);
            } catch (error) {
                logger.except("向表 [" +  EXTENDCLNAME + "] 中插入数据失败", error);
                throw error;
            }
        }
    } catch (error) {
        throw error;
    } finally {
        cursor.close();
    }
    return subCLsize;
}

function getCLInfo() {
    try {
        // 找出匹配的 SNAPSHOT_CL
        try {
            if (ISPRIMARY) {
                // 带有 nodeselect = "primary"
                //logger.debug('insert into ' + DATACS + '.' + MATCHCLNAME + ' select b.Name,b.CollectionSpace,b.Details from ' + DATACS + '.' + MATCHCATANAME + ' as a inner join ' + DATACS + '.' + SNAPSHOTCLNAME + ' as b on a.Name = b.Name /*+use_hash()*/');
                db.execUpdate('insert into ' + DATACS + '.' + MATCHCLNAME + ' select b.Name,b.CollectionSpace,b.Details from ' + DATACS + '.' + MATCHCATANAME + ' as a inner join ' + DATACS + '.' + SNAPSHOTCLNAME + ' as b on a.Name = b.Name /*+use_hash()*/');
            } else {
                // 不带有 nodeselect = "primary" 
                db.execUpdate('insert into ' + DATACS + '.' + MATCHCLNAME + ' select b.Name,b.CollectionSpace,b.Details from ' + DATACS + '.' + MATCHCATANAME + ' as a inner join ' + DATACS + '.' + SNAPSHOTCLNAME + ' as b on a.Name = b.Name /*+use_hash()*/');
            }
        } catch (error) {
            logger.except("根据表 [" + DATACS + "." + MATCHCATANAME + "] 中匹配命中的表去 [" + DATACS + "." + SNAPSHOTCLNAME + "] 中查找 SNAPSHOT_CL 信息失败", error);
            throw error;
        }

        let cursor;
        try {
            if (ISPRIMARY) {
                // 带有 nodeselect = "primary"
                cursor = db.exec('select t.Details.TotalRecords as TotalRecords,t.Details.TotalLobs as TotalLobs,t.Details.TotalDataPages as TotalDataPages,t.Details.TotalIndexPages as TotalIndexPages ,t.Details.TotalDataFreeSpace as TotalDataFreeSpace,t.Details.TotalIndexFreeSpace as TotalIndexFreeSpace,t.Details.PageSize as PageSize, \
                t.Details.GroupName as GroupName,t.Details.TotalLobPages as TotalLobPages,t.Details.LobPageSize as LobPageSize,t.Name from (select Details,Name from ' + DATACS + '.' + MATCHCLNAME + ' split by Details) as t');
            } else {
                // 不带有 nodeselect = "primary" 
                cursor = db.exec('select t.Details.TotalRecords as TotalRecords,t.Details.TotalLobs as TotalLobs,t.Details.TotalDataPages as TotalDataPages,t.Details.TotalIndexPages as TotalIndexPages ,t.Details.TotalDataFreeSpace as TotalDataFreeSpace,t.Details.TotalIndexFreeSpace as TotalIndexFreeSpace,t.Details.PageSize as PageSize, \
                t.Details.GroupName as GroupName,t.Details.TotalLobPages as TotalLobPages,t.Details.LobPageSize as LobPageSize,t.Name from (select Details,Name from ' + DATACS + '.' + MATCHCLNAME + ' split by Details) as t group by t.Name,t.Details.GroupName');
            }
        } catch (error) {
            logger.except("获取表 [" + DATACS + "." + MATCHCLNAME + "] 中数据失败", error);
            throw error;
        }

        try {
            let cl = db.getCS(DATACS).getCL(GROUPSIZENAME);
            let loop = 0;
            while (cursor.next()) {
                loop++;
                if ((loop % INFOOUTPUTONCE) == 0) {
                    logger.info("已计算 " + loop + " 个组的数据量");
                }
                let current = cursor.current().toObj();
                let insertObj = {};
                let currentGroupSizeGB = ((current.TotalDataPages + current.TotalIndexPages) * current.PageSize - current.TotalDataFreeSpace - current.TotalIndexFreeSpace) / 1024 / 1024 / 1024;
                insertObj['currentGroupSizeGB'] = currentGroupSizeGB;
                insertObj['currentLobSizeGB'] = (current.TotalLobPages * current.LobPageSize) / 1024 / 1024 / 1024;
                insertObj['GroupName'] = current.GroupName;
                insertObj['Name'] = current.Name;
                insertObj['groupTotalRecords'] = current.TotalRecords;
                insertObj['groupTotalLobs'] = current.TotalLobs;
                //logger.debug(JSON.stringify(insertObj));
                cl.insert(insertObj);
            }
        } catch (error) {
            logger.except("向表 [" + DATACS + "." + GROUPSIZENAME  + "] 插入数据失败", error);
            throw error;
        } finally {
            cursor.close();
        }

        logger.info("计算完成，开始合并信息");
        try {
            db.execUpdate('insert into ' + DATACS + "." + CLSIZENAME + ' select sum(groupTotalRecords) as totalRecords,sum(groupTotalLobs) as totalLobs,sum(currentGroupSizeGB) as totalSizeGB, \
            sum(currentLobSizeGB) as totalLobSizeGB, max(currentGroupSizeGB) as groupMaxSizeGB, \
            min(currentGroupSizeGB) as groupMinSizeGB, count(GroupName) as groupNum, Name, push(GroupName) as groupArray, \
            max(currentLobSizeGB) as groupLobMaxSizeGB, min(currentLobSizeGB) as groupLobMinSizeGB\
            from ' + DATACS + "." + GROUPSIZENAME + ' group by Name');
        } catch (error) {
            logger.except("对表 [" + DATACS + "." + GROUPSIZENAME  + "] 进行数据聚集运算，并插入到表 [" + DATACS + "." + CLSIZENAME  + "] 失败", error);
            throw error;
        }
    } catch (error) {
        logger.except("获取 CL 详细信息失败", error);
        throw error;
    }

    // 建之前几个表的信息做一个合并到新表中，以便最后输出时不用 inner join, join 字段都是完整的表名 Name
    // 从 MATCH_CATA 中取出 Name,MainCLName,appName
    // 从 LIST_CS 中取出 PageSize,LobPageSize,domain
    // 从 CL_SIZE 中取出 totalSizeGB,groupMaxSizeGB,groupMinSizeGB,groupNum,groupArray

    // 先通过 MATCH_CL 和 LIST_CS 记录 CL 与 CS 中 PageSize,LobPageSize,domain 的关系，插入到 HRBRID_CS_CL 中
    try {
        db.execUpdate('insert into ' + DATACS + '.' + HYBRIDCSCLNAME + ' \
        select b.Name,b.CollectionSpace as CSName,a.PageSize as pageSize,a.LobPageSize as lobPageSize,a.Domain as domain \
        from (select PageSize,LobPageSize,Name,Domain from ' + DATACS + '.' + LISTCSNAME + ') \
        as a inner join (select Name,CollectionSpace from ' + DATACS + '.' + MATCHCLNAME + ' group by Name) as b \
        on a.Name = b.CollectionSpace order by b.Name /*+use_hash()*/');
    } catch (error) {
        logger.except("无法合并表 [" + DATACS + "." + LISTCSNAME + " " + DATACS + "." + MATCHCLNAME + "] 数据到 [" + DATACS + "." + HYBRIDCSCLNAME + "] 中", error);
        throw error;
    }

    // 然后从 MATCH_CATA 中取出 Name,MainCLName,appName,从 CL_SIZE 中取出 totalSizeGB,groupMaxSizeGB,groupMinSizeGB,groupNum,groupArray，从 MAIN_MAX 中取出 maxCLName，先合并到临时表
    try {
        db.execUpdate('insert into ' + DATACS + "." + TMPCLNAME + ' \
        select t1.Name,t1.MainCLName,t1.appName,t1.modelType,t1.Partition,t1.totalRecords,t1.totalLobs,t1.totalSizeGB,t1.totalLobSizeGB,t1.groupMaxSizeGB,t1.groupMinSizeGB,t1.groupLobMaxSizeGB,t1.groupLobMinSizeGB,t1.groupNum,t1.groupArray,t2.maxCLName from \
        (select a.Name,a.MainCLName,a.appName,a.modelType,a.Partition,b.totalRecords,b.totalLobs,b.totalSizeGB,b.totalLobSizeGB,b.groupMaxSizeGB,b.groupMinSizeGB,b.groupLobMaxSizeGB,b.groupLobMinSizeGB,b.groupNum,b.groupArray from ' + DATACS + '.' + MATCHCATANAME + ' as a \
        inner join ' + DATACS + '.' + CLSIZENAME + ' as b on a.Name = b.Name /*+use_hash()*/) as t1 left outer join ' + DATACS + '.' + MAINMAXNAME + ' as t2 on t1.MainCLName = t2.MainCLName /*+use_hash()*/');
    } catch (error) {
        logger.except("无法合并表 [" + DATACS + "." + MATCHCATANAME + " " + DATACS + "." + CLSIZENAME + " " + DATACS + "." + MAINMAXNAME + "] 数据到 [" + DATACS + "." + TMPCLNAME + "] 中", error);
        throw error;
    }

    // 然后把最终记录合并到 OUTPUT 表
    try {
        db.execUpdate('insert into ' + DATACS + '.' + OUTOUTCLNAME + ' \
        select t1.Name,t1.CSName,t1.pageSize,t1.lobPageSize,t1.domain,t1.MainCLName,t1.maxCLName,t1.appName,t1.modelType,t1.Partition,t1.totalRecords,t1.totalLobs,t1.totalSizeGB,t1.totalLobSizeGB,t1.groupMaxSizeGB,t1.groupMinSizeGB,t1.groupLobMaxSizeGB,t1.groupLobMinSizeGB,t1.groupNum,t1.groupArray,\
        t2.lastCLName,t2.secondCLName from (select a.Name,a.CSName,a.pageSize,a.lobPageSize,a.domain,b.MainCLName,b.maxCLName,b.appName,b.modelType,b.Partition,b.totalRecords,b.totalLobs,b.totalSizeGB,b.totalLobSizeGB,b.groupMaxSizeGB,b.groupMinSizeGB,b.groupLobMaxSizeGB,b.groupLobMinSizeGB,b.groupNum,b.groupArray \
        from ' + DATACS + '.' + HYBRIDCSCLNAME + ' as a inner join ' + DATACS + "." + TMPCLNAME + ' as b on a.Name = b.Name /*+use_hash()*/) as t1 \
        inner join ' + DATACS + "." + LASTCLNAME + ' as t2 on t1.modelType = t2.modelType and t1.appName = t2.appName /*+use_hash()*/')
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
        // totalSizeGB,groupAvgSizeGB,groupMaxSizeGB,groupMinSizeGB,Max/Min 的计算是根据 totalRecord,totalLobs 中不为 0 的一项计算的，如果两项都不为0，会出现重复
        CLCsv = new File(CLCSV);
        CLCsv.write("CLName,mainCLName,appName,domain,totalRecord,totalLobs,totalSizeGB,groupAvgSizeGB,groupMaxSizeGB,groupMinSizeGB,Max/Min,groupNum,groups" + '\n');
        // 输出表类型维度 csv: 表类型(主表)，应用名，最后一个时间分区增量数据，时间分区平均数据量，时间分区最大数据量，时间分区最少数据量，最大/最小偏差值，总数据量，总数据组，表个数，总分区数
        CLTypeCsv = new File(CLTYPECSV);
        CLTypeCsv.write("type,appName,lastCLName,lastTimeSizeGB,timeAvgSizeGB,timeMaxSizeGB,timeMinSizeGB,timeMax/timeMin,totalSizeGB,totalGroupNum,CLCount,totalPartNum" + '\n');
        // 输出表类型维度 csv: 应用名，主表名，创建表名，表类型，域，组个数，挂载字段，挂载上下界，卸载的 MAX 表，pagesize，logpagesize，partition, 索引使用通用还是继承 ,是否主表
        addCLCsv = new File(ADDCLCSV);
        addCLCsv.write("appName,mainCLName,createCLName,type,domain,groupNum,shardingKey,lowBound,upBound,detachCL,pageSize,lobPageSize,partition,indexType,isMainCL" + '\n');
    } catch (error) {
        logger.except("文件写入出错", error);
        throw error;
    }

    let loop = 0;
    let cursor;
    // 输出表维度 information_by_cl.csv
    try {
        try {
            cursor = db.exec('select * from ' + DATACS + "." + OUTOUTCLNAME);
        } catch (error) {
            logger.except("获取表 [" + DATACS + "." + OUTOUTCLNAME + "] 中数据失败", error);
            throw error;
        }
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
            // 区分 LOB 表和元数据表
            if (current.totalRecords != 0) {
                CLCsvLine.push(current.totalSizeGB.toFixed(FIXNUM));
                // avg 不准确，因为 groupNum 没有排除空表
                CLCsvLine.push((current.totalSizeGB/current.groupNum).toFixed(FIXNUM));
                CLCsvLine.push(current.groupMaxSizeGB.toFixed(FIXNUM));
                CLCsvLine.push(current.groupMinSizeGB.toFixed(FIXNUM));
                if ((current.groupMaxSizeGB - current.groupMinSizeGB) * 1024 > MAX_MIN) {
                    CLCsvLine.push((current.groupMaxSizeGB/current.groupMinSizeGB).toFixed(FIXNUM));
                } else {
                    CLCsvLine.push(0);
                }
            } else if (current.totalLobs != 0) {
                CLCsvLine.push(current.totalLobSizeGB.toFixed(FIXNUM));
                // avg 不准确，因为 groupNum 没有排除空表
                CLCsvLine.push((current.totalLobSizeGB/current.groupNum).toFixed(FIXNUM));
                CLCsvLine.push(current.groupLobMaxSizeGB.toFixed(FIXNUM));
                CLCsvLine.push(current.groupLobMinSizeGB.toFixed(FIXNUM));
                if ((current.groupLobMaxSizeGB - current.groupLobMinSizeGB) * 1024 > MAX_MIN) {
                    CLCsvLine.push((current.groupLobMaxSizeGB/current.groupLobMinSizeGB).toFixed(FIXNUM));
                } else {
                    CLCsvLine.push(0);
                }
            } else {
                for (let i = 0; i < 5; i++) {
                    CLCsvLine.push(0);
                }
            }

            CLCsvLine.push(current.groupNum);
            CLCsvLine.push(current.groupArray.sort().join('$'));
            CLCsv.write(CLCsvLine.join(',') + "\n");
        }
    } catch (error) {
        logger.except("输出 " + CLCSV + " 文件失败", error);
        throw error;
    } finally {
        cursor.close();
    }

    try {
        // 表类型维度 information_by_cltype.csv
        // lastCL 取的是上一个时间分区的表，因为此时间的数据已经稳定，可以体现增量
        try {
            if (SKIPEMPTYBYLASTCL) {
                // cursor = db.exec('select appName,modelType,last(Name) as lastCLName,last(totalSizeGB) as lastTimeSizeGB,sum(totalSizeGB) as totalSizeGB,count(Name) as totalPartNum,max(totalSizeGB) as timeMaxSizeGB, min(totalSizeGB) as timeMinSizeGB\
                // ,groupNum,last(totalRecords) as lastTimeRecords,last(totalLobs) as lastTimeLobs,last(totalLobSizeGB) as lastTimeLobSizeGB,sum(totalLobSizeGB) as totalLobSizeGB,max(totalLobSizeGB) as timeMaxLobSizeGB, min(totalLobSizeGB) as timeMinLobSizeGB \
                // from ' + DATACS + "." + OUTOUTCLNAME + ' where totalRecords <> 0 or totalLobs <> 0 group by appName,modelType');
                cursor = db.exec('select b.Name as lastCLName,b.totalSizeGB as lastTimeSizeGB,b.totalRecords as lastTimeRecords,b.totalLobs as lastTimeLobs,b.totalLobSizeGB as lastTimeLobSizeGB,\
                a.appName,a.modelType,a.totalSizeGB,a.totalPartNum,a.timeMaxSizeGB,a.timeMinSizeGB,a.groupNum,a.totalLobSizeGB,a.timeMaxLobSizeGB,a.timeMinLobSizeGB \
                from (select appName,modelType,sum(totalSizeGB) as totalSizeGB,count(Name) as totalPartNum,max(totalSizeGB) as timeMaxSizeGB, min(totalSizeGB) as timeMinSizeGB\
                ,groupNum,sum(totalLobSizeGB) as totalLobSizeGB,max(totalLobSizeGB) as timeMaxLobSizeGB, min(totalLobSizeGB) as timeMinLobSizeGB,secondCLName \
                from ' + DATACS + "." + OUTOUTCLNAME + ' where totalRecords <> 0 or totalLobs <> 0 group by appName,modelType) as a inner join ' + DATACS + "." + OUTOUTCLNAME + ' as b on a.secondCLName = b.Name /*+use_hash()*/');
            } else {
                // cursor = db.exec('select appName,modelType,last(Name) as lastCLName,last(totalSizeGB) as lastTimeSizeGB,sum(totalSizeGB) as totalSizeGB,count(Name) as totalPartNum,max(totalSizeGB) as timeMaxSizeGB, min(totalSizeGB) as timeMinSizeGB\
                // ,groupNum,last(totalRecords) as lastTimeRecords,last(totalLobs) as lastTimeLobs,last(totalLobSizeGB) as lastTimeLobSizeGB,sum(totalLobSizeGB) as totalLobSizeGB,max(totalLobSizeGB) as timeMaxLobSizeGB, min(totalLobSizeGB) as timeMinLobSizeGB \
                // from ' + DATACS + "." + OUTOUTCLNAME + ' group by appName,modelType');
                cursor = db.exec('select b.Name as lastCLName,b.totalSizeGB as lastTimeSizeGB,b.totalRecords as lastTimeRecords,b.totalLobs as lastTimeLobs,b.totalLobSizeGB as lastTimeLobSizeGB,\
                a.appName,a.modelType,a.totalSizeGB,a.totalPartNum,a.timeMaxSizeGB,a.timeMinSizeGB,a.groupNum,a.totalLobSizeGB,a.timeMaxLobSizeGB,a.timeMinLobSizeGB \
                from (select appName,modelType,sum(totalSizeGB) as totalSizeGB,count(Name) as totalPartNum,max(totalSizeGB) as timeMaxSizeGB, min(totalSizeGB) as timeMinSizeGB\
                ,groupNum,sum(totalLobSizeGB) as totalLobSizeGB,max(totalLobSizeGB) as timeMaxLobSizeGB, min(totalLobSizeGB) as timeMinLobSizeGB,secondCLName \
                from ' + DATACS + "." + OUTOUTCLNAME + ' group by appName,modelType) as a inner join ' + DATACS + "." + OUTOUTCLNAME + ' as b on a.secondCLName = b.Name /*+use_hash()*/');
            }
        } catch (error) {
            logger.except("获取表 [" + DATACS + "." + OUTOUTCLNAME + "] 中聚合数据失败", error);
            throw error;
        }
        while(cursor.next()) {
            loop++;
            if (loop % INFOOUTPUTONCE == 0) {
                logger.info("已输出 " + loop + " 条信息");
            }
            let CLTypeLine = [];
            let current = cursor.current().toObj();
            CLTypeLine.push(current.modelType);
            CLTypeLine.push(current.appName);
            CLTypeLine.push(current.lastCLName);
            // 区分 LOB 表和元数据表
            if (current.lastTimeRecords != 0) {
                CLTypeLine.push(current.lastTimeSizeGB.toFixed(FIXNUM));
                // 这里算的 timeAvgSizeGB 的 totalPartNum 会算上空表，导致没什么意义，后续需要剔除空表再算
                CLTypeLine.push((current.totalSizeGB / current.totalPartNum).toFixed(FIXNUM));
                CLTypeLine.push(current.timeMaxSizeGB.toFixed(FIXNUM));
                CLTypeLine.push(current.timeMinSizeGB.toFixed(FIXNUM));
                CLTypeLine.push((current.timeMaxSizeGB / current.timeMinSizeGB).toFixed(FIXNUM));
                CLTypeLine.push(current.totalSizeGB.toFixed(FIXNUM));
            } else if (current.lastTimeLobs != 0) {
                CLTypeLine.push(current.lastTimeLobSizeGB.toFixed(FIXNUM));
                CLTypeLine.push((current.totalLobSizeGB / current.totalPartNum).toFixed(FIXNUM));
                CLTypeLine.push(current.timeMaxLobSizeGB.toFixed(FIXNUM));
                CLTypeLine.push(current.timeMinLobSizeGB.toFixed(FIXNUM));
                CLTypeLine.push((current.timeMaxLobSizeGB / current.timeMinLobSizeGB).toFixed(FIXNUM));
                CLTypeLine.push(current.totalLobSizeGB.toFixed(FIXNUM));
            }
            CLTypeLine.push(current.groupNum);
            // 普通表表个数等于分区数
            CLTypeLine.push(current.totalPartNum);
            CLTypeLine.push(current.totalPartNum);
            CLTypeCsv.write(CLTypeLine.join(',') + "\n");
        }
    } catch (error) {
        logger.except("输出 " + CLTYPECSV + " 文件失败", error);
        throw error;
    } finally {
        cursor.close();
    }

    try {
        // 扩展表维度
        try {
            cursor = db.exec('select b.appName,b.MainCLName,a.createCLName,b.modelType,b.domain,b.groupNum,a.shardingKeyObj,a.lowBound,a.upBound,b.maxCLName,b.pageSize,b.lobPageSize,b.Partition \
            from ' + DATACS + "." + EXTENDCLNAME + ' as a inner join ' + DATACS + "." + OUTOUTCLNAME + ' as b on a.prevCLName = b.Name /*+use_hash()*/')
        } catch (error) {
            logger.except("获取表 [" + DATACS + "." + EXTENDCLNAME + "] 和 [" + DATACS + "." + OUTOUTCLNAME + "] 的聚合数据失败", error);
            throw error;
        }
        while(cursor.next()) {
            loop++;
            if (loop % INFOOUTPUTONCE == 0) {
                logger.info("已输出 " + loop + " 条信息");
            }
            let addCLCsvLine = [];
            let current = cursor.current().toObj();
            addCLCsvLine.push(current.appName);
            addCLCsvLine.push(current.MainCLName);
            addCLCsvLine.push(current.createCLName);
            addCLCsvLine.push(current.modelType);
            addCLCsvLine.push(current.domain);
            addCLCsvLine.push(current.groupNum);
            if (current.shardingKeyObj != undefined) {
                addCLCsvLine.push(Object.keys(current.shardingKeyObj));
            } else {
                addCLCsvLine.push("");
            }
            addCLCsvLine.push(current.lowBound);
            addCLCsvLine.push(current.upBound);
            addCLCsvLine.push(current.maxCLName);
            addCLCsvLine.push(current.pageSize);
            addCLCsvLine.push(current.lobPageSize);
            addCLCsvLine.push(current.Partition);
            addCLCsvLine.push("inherit");
            addCLCsvLine.push(false);
            addCLCsv.write(addCLCsvLine.join(',') + "\n");
        }
    } catch (error) {
        logger.except("输出 " + ADDCLCSV + " 文件失败", error);
        throw error;
    } finally {
        cursor.close();
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

    let modelJson = {};
    let model_cl_array = [];
    let CLObj = {};

    // MAX 表，需要所有主子表都有或没有，如果部分有，部分没有，为 hybrid
    try {
        let emptyMaxCLSize;
        let allMaxCLSize;
        try {
            emptyMaxCLSize = db.exec('select maxCLName from ' + DATACS + '.' + OUTOUTCLNAME + ' where MainCLName is not null and maxCLName is null').size();
            allMaxCLSize = db.exec('select maxCLName from ' + DATACS + '.' + OUTOUTCLNAME + ' where MainCLName is not null').size();
        } catch (error) {
            logger.except("获取表 [" + DATACS + "." + OUTOUTCLNAME + "] 中拥有 MAX 分区的主表数据失败", error);
        }
        if (emptyMaxCLSize == allMaxCLSize) {
            modelJson["include_max_range"] = false;
        } else if (emptyMaxCLSize == 0) {
            // 此处 allMaxCLSize 不会为 0
            modelJson["include_max_range"] = true;
        } else {
            modelJson["include_max_range"] = "hybrid";
            logger.warn("当前模型中部分主子表挂载了 MAX 表，部分主子未挂载 MAX 表");
        }
    } catch (error) {
        logger.except("获取模型 MAX 分区是否存在失败", error);
        throw error;
    }

    let cursor;
    // 主表
    try {
        try {
            cursor = db.exec('select a.modelType,a.findCondition,a.ShardingType,a.ShardingKey,b.lowBound from \
            (select modelType,findCondition,ShardingType,ShardingKey,Name from ' + DATACS + '.' + MATCHCATANAME + ' where IsMainCL = true group by modelType) as a \
            inner join (select mainCLName,lowBound from ' + DATACS + '.' + EXTENDCLNAME + ' group by mainCLName) as b on a.Name = b.mainCLName /*+use_hash()*/');
        } catch (error) {
            logger.except("获取表 [" + DATACS + "." + EXTENDCLNAME + "] 和 [" + DATACS + "." + MATCHCATANAME + "] 的聚合数据失败", error);
            throw error;
        }
        while (cursor.next()) {
            let current = cursor.current().toObj();
            CLObj['model'] = current.findCondition;
            CLObj['type'] = current.modelType;
            let shardingkey = {};
            shardingkey["type"] = current.ShardingType;
            // 有些情况可能无法处理
            if (current.lowBound.length == 4) {
                let tmpObj = current.ShardingKey;
                let key = Object.keys(tmpObj);
                tmpObj[key] = "$YYYY"
                shardingkey["key"] = tmpObj;
            } else if (current.lowBound.length == 6) {
                let tmpObj = current.ShardingKey;
                let key = Object.keys(tmpObj);
                tmpObj[key] = "$YYYY$MM"
                shardingkey["key"] = tmpObj;
            } else if (current.lowBound.length == 8) {
                let tmpObj = current.ShardingKey;
                let key = Object.keys(tmpObj);
                tmpObj[key] = "$YYYY$MM$dd"
                shardingkey["key"] = tmpObj;
            } else if (current.lowBound.length == 14) {
                let tmpObj = current.ShardingKey;
                let key = Object.keys(tmpObj);
                tmpObj[key] = "$YYYY$MM$dd$HH$mm$ss"
                shardingkey["key"] = tmpObj;
            } else {
                shardingkey["key"] = current.ShardingKey;
            }
            CLObj['shardingkey'] = shardingkey;
            model_cl_array.push(CLObj);
            CLObj = {};
        }
        modelJson['main_cl'] = model_cl_array;
        model_cl_array = [];
    } catch (error) {
        logger.except("获取主表模型信息失败", error);
        throw error;
    } finally {
        cursor.close();
    }

    // 子表
    try {
        try {
            cursor = db.exec('select a.appName,a.modelType,a.ShardingType,a.ShardingKey,a.MainCLName,b.subCLFindCondtiton from \
            (select appName,modelType,ShardingType,ShardingKey,MainCLName from ' + DATACS + '.' + MATCHCATANAME + ' where IsMainCL is null and MainCLName is not null group by modelType) \
            as a inner join ' + DATACS + '.' + SUBCLFINDCONDCLNAME + ' as b on a.MainCLName = b.mainCLName group by a.modelType,b.subCLFindCondtiton /*+use_hash()*/');
        } catch (error) {
            logger.except("获取表 [" + DATACS + "." + MATCHCATANAME + "] 和 [" + DATACS + "." + SUBCLFINDCONDCLNAME + "] 的聚合数据失败", error);
            throw error;
        }
        while (cursor.next()) {
            let current = cursor.current().toObj();
            CLObj['model'] = current.subCLFindCondtiton.replace(new RegExp(current.appName, 'g'), "$APPNAME");
            CLObj['type'] = current.modelType;
            let shardingkey = {};
            shardingkey["type"] = current.ShardingType;
            shardingkey["key"] = current.ShardingKey;
            CLObj['shardingkey'] = shardingkey;
            model_cl_array.push(CLObj);
            CLObj = {};
        }
        modelJson['sub_cl'] = model_cl_array;
        model_cl_array = [];
    } catch (error) {
        logger.except("获取子表模型信息失败", error);
        throw error;
    } finally {
        cursor.close();
    }

    // 普通表
    try {
        try {
            cursor = db.exec('select modelType,findCondition,ShardingType,ShardingKey from ' + DATACS + '.' + MATCHCATANAME + ' where IsMainCL is null and MainCLName is null group by modelType,findCondition');
        } catch (error) {
            logger.except("获取表 [" + DATACS + "." + MATCHCATANAME + "] 中数据失败", error);
            throw error;
        }
        while (cursor.next()) {
            let current = cursor.current().toObj();
            CLObj['model'] = current.findCondition;
            CLObj['type'] = current.modelType;
            let shardingkey = {};
            shardingkey["type"] = current.ShardingType;
            shardingkey["key"] = current.ShardingKey;
            CLObj['shardingkey'] = shardingkey;
            model_cl_array.push(CLObj);
            CLObj = {};
        }
        modelJson['normal_cl'] = model_cl_array;
        model_cl_array = [];
    } catch (error) {
        logger.except("获取普通表模型信息失败", error);
        throw error;
    } finally {
        cursor.close();
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
    logger.info("开始对比标准模型");
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

function getFrameWork() {
    let frameWork;
    try {
        let size = db.exec('select * from ' + DATACS + '.' + LISTCSNAME + ' where Name = "SCMSYSTEM"').size();
        if (size == 1) {
            frameWork = "SCM";
        } else if (size == 0) {
            frameWork = "SDB";
        } else {
            frameWork = "";
        }
    } catch (error) {
        logger.except("在 CS [" + DATACS + '.' + LISTCSNAME + "] 中检查 CS [SCMSYSTEM] 是否存在失败", error);
        throw error;
    }

    return frameWork;
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
    outputModel();
    logger.info("模型信息输出完成");
    logger.info("模型信息文件: " + CURRENTMODELJSON);
    checkModel();
    logger.info("模型匹配完成");
}

function getSnapshot() {
    let cmd;
    try {
        cmd = new Cmd();
    } catch (error) {
        logger.except("获取 Cmd() 失败", error);
        throw error;
    }

    logger.info("开始获取 $SNAPSHOT_CATA 数据到文件 " + SNAPSHOTCATAFILE + " 中");
    try {
        cmd.run('sdb "db = new Sdb(\\"' + COORDADDR + '\\",' + COORDSVC + ',\\"' + DBUSER + '\\",\\"' + DBPASSWORD + '\\")";sdb "db.exec(\\"select * from \\$SNAPSHOT_CATA\\")" > ' + SNAPSHOTCATAFILE);
        cmd.run('sed -i "\\$d" ' + SNAPSHOTCATAFILE);
    } catch (error) {
        logger.except("查询 $SNAPSHOT_CATA 数据导出到文件 " + SNAPSHOTCATAFILE + " 失败", error);
        throw error;
    }

    logger.info("获取完成，开始获取 $SNAPSHOT_CL 数据到文件 " + SNAPSHOTCLFILE + " 中");
    try {
        if (ISPRIMARY) {
            cmd.run('sdb "db = new Sdb(\\"' + COORDADDR + '\\",' + COORDSVC + ',\\"' + DBUSER + '\\",\\"' + DBPASSWORD + '\\")";sdb "db.exec(\\"select * from \\$SNAPSHOT_CL where nodeselect = \\\\\\"primary\\\\\\"\\")" > ' + SNAPSHOTCLFILE);
        } else {
            cmd.run('sdb "db = new Sdb(\\"' + COORDADDR + '\\",' + COORDSVC + ',\\"' + DBUSER + '\\",\\"' + DBPASSWORD + '\\")";sdb "db.exec(\\"select * from \\$SNAPSHOT_CL\\")" > ' + SNAPSHOTCLFILE);
        }
        cmd.run('sed -i "\\$d" ' + SNAPSHOTCLFILE);
    } catch (error) {
        logger.except("查询 $SNAPSHOT_CL 数据导出到文件 " + SNAPSHOTCLFILE + " 失败", error);
        throw error;
    }

    logger.info("获取完成，开始获取 $SNAPSHOT_CS 数据到文件 " + SNAPSHOTCSFILE + " 中");
    try {
        if (ISPRIMARY) {
            cmd.run('sdb "db = new Sdb(\\"' + COORDADDR + '\\",' + COORDSVC + ',\\"' + DBUSER + '\\",\\"' + DBPASSWORD + '\\")";sdb "db.exec(\\"select * from \\$SNAPSHOT_CS where nodeselect = \\\\\\"primary\\\\\\"\\")" > ' + SNAPSHOTCSFILE);
        } else {
            cmd.run('sdb "db = new Sdb(\\"' + COORDADDR + '\\",' + COORDSVC + ',\\"' + DBUSER + '\\",\\"' + DBPASSWORD + '\\")";sdb "db.exec(\\"select * from \\$SNAPSHOT_CS\\")" > ' + SNAPSHOTCSFILE);
        }
        cmd.run('sed -i "\\$d" ' + SNAPSHOTCSFILE);
    } catch (error) {
        logger.except("查询 $SNAPSHOT_CS 数据导出到文件 " + SNAPSHOTCSFILE + " 失败", error);
        throw error;
    }

    logger.info("获取完成，开始获取 $LIST_CS 数据到文件 " + LISTCSFILE + " 中");
    try {
        cmd.run('sdb "db = new Sdb(\\"' + COORDADDR + '\\",' + COORDSVC + ',\\"' + DBUSER + '\\",\\"' + DBPASSWORD + '\\")";sdb "db.exec(\\"select * from \\$LIST_CS\\")" > ' + LISTCSFILE);
        cmd.run('sed -i "\\$d" ' + LISTCSFILE);
    } catch (error) {
        logger.except("查询 $LIST_CS 数据导出到文件 " + LISTCSFILE + " 失败", error);
        throw error;
    }

    logger.info("获取完成，开始获取 $SNAPSHOT_SYSTEM 数据到文件 " + SNAPSHOTSYSTEMFILE + " 中");
    try {
        cmd.run('sdb "db = new Sdb(\\"' + COORDADDR + '\\",' + COORDSVC + ',\\"' + DBUSER + '\\",\\"' + DBPASSWORD + '\\")";sdb "db.exec(\\"select * from \\$SNAPSHOT_SYSTEM\\")" > ' + SNAPSHOTSYSTEMFILE);
        cmd.run('sed -i "\\$d" ' + SNAPSHOTSYSTEMFILE);
    } catch (error) {
        logger.except("查询 $SNAPSHOT_SYSTEM 数据导出到文件 " + SNAPSHOTSYSTEMFILE + " 失败", error);
        throw error;
    }

    logger.info("获取完成");
}

function importSnapshotFile() {
    let cmd;
    try {
        cmd = new Cmd();
    } catch (error) {
        logger.except("获取 Cmd() 失败", error);
        throw error;
    }

    try {
        cmd.run('ls ' + SNAPSHOTCATAFILE);
    } catch (error) {
        logger.except("$SNAPSHOT_CATA 数据文件 " + SNAPSHOTCATAFILE + " 不存在", error);
        throw error;
    }

    try {
        cmd.run('ls ' + SNAPSHOTCLFILE);
    } catch (error) {
        logger.except("$SNAPSHOT_CL 数据文件 " + SNAPSHOTCLFILE + " 不存在", error);
        throw error;
    }

    try {
        cmd.run('ls ' + LISTCSFILE);
    } catch (error) {
        logger.except("$LIST_CS 数据文件 " + LISTCSFILE + " 不存在", error);
        throw error;
    }

    logger.info("导入 $SNAPSHOT_CATA 数据文件 " + SNAPSHOTCATAFILE + " 到表 " + SNAPSHOTCATANAME + " 中");
    try {
        cmd.run('sdbimprt --hosts "' + COORDADDR + ':' + COORDSVC + '" --type json -c ' + DATACS + ' -l ' + SNAPSHOTCATANAME + ' --user ' + DBUSER + ' --password ' + DBPASSWORD + ' --file ' + SNAPSHOTCATAFILE);
    } catch (error) {
        logger.except("导入 $SNAPSHOT_CATA 数据文件 " + SNAPSHOTCATAFILE + " 到表 " + SNAPSHOTCATANAME + " 失败", error);
        throw error;
    }

    logger.info("导入 $SNAPSHOT_CL 数据文件 " + SNAPSHOTCLFILE + " 到表 " + SNAPSHOTCLNAME + " 中");
    try {
        cmd.run('sdbimprt --hosts "' + COORDADDR + ':' + COORDSVC + '" --type json -c ' + DATACS + ' -l ' + SNAPSHOTCLNAME + ' --user ' + DBUSER + ' --password ' + DBPASSWORD + ' --file ' + SNAPSHOTCLFILE);
    } catch (error) {
        logger.except("导入 $SNAPSHOT_CL 数据文件 " + SNAPSHOTCLFILE + " 到表 " + SNAPSHOTCLNAME + " 失败", error);
        throw error;
    }

    logger.info("导入 $LIST_CS 数据文件 " + LISTCSFILE + " 到表 " + LISTCSNAME + " 中");
    try {
        cmd.run('sdbimprt --hosts "' + COORDADDR + ':' + COORDSVC + '" --type json -c ' + DATACS + ' -l ' + LISTCSNAME + ' --user ' + DBUSER + ' --password ' + DBPASSWORD + ' --file ' + LISTCSFILE);
    } catch (error) {
        logger.except("导入 $LIST_CS 数据文件 " + LISTCSFILE + " 到表 " + LISTCSNAME + " 失败", error);
        throw error;
    }
}

function start() {
    try {
        let frameWork = getFrameWork();
        if (frameWork == "SDB") {
            logger.info("当前业务部署架构为: SDB");
        } else if (frameWork == "SCM") {
            logger.info("当前业务部署架构为: SCM");
        } else {
            logger.warn("无法确定当前业务部署架构");
        }
    
        logger.info("开始解析主表名");
        // 获取主表名
        let mainCLSize = findCL(MAIN_CL_ARRAY, "MainSubCL");
        logger.info("解析主表完成，主表数量为：" + mainCLSize);
        
        logger.info("开始解析普通表名");
        // 获取普通表名
        let normalCLSize = findCL(NORMAL_CL_ARRAY, "NormalCL");
        logger.info("解析普通表完成，普通表数量为：" + normalCLSize);
    
        logger.info("开始获取子表");
        // 获取全量子表
        let fullSubCLSize = getFullSubCLInfo();
        logger.info("获取全部子表完成，数量为：" + fullSubCLSize);
    
        // 获取 CL 下的其他信息
        logger.info("开始获取表信息");
        getNormalCLExtendName();
        getCLInfo();
        logger.info("获取表信息完成，开始生成文件");
        // 输出文件
        outputFile();
    } catch (error) {
        throw error;
    }
}

function checkAllKeyWord() {
    if (!checkKeyWord(MAIN_CL_ARRAY)) {
        return false;
    }
    if (!checkKeyWord(NORMAL_CL_ARRAY)) {
        return false;
    }
    if (!checkKeyWord(DATE_FORMAT_ARRAY)) {
        return false;
    }
    return true;
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
            return false;
        }
    }
    return true;
}

function initDataCL(CLName) {
    try {
        db.getCS(DATACS).createCL(CLName);
    } catch (error) {
        if (error == -22) {
            db.getCS(DATACS).getCL(CLName).truncate();
        } else {
            logger.except("在 DATACS [" + DATACS + "] 下创建 [" + CLName + "] 失败", error);
            throw error;
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
        let cursor = db.getCS(DATACS).listCollections();
        while (cursor.next()) {
            let current = cursor.current().toObj();
            if (-1 == TOOLCLARRAY.indexOf(current['Name'].split('.')[1])) {
                let content = "在 DATACS [" + DATACS + "] 中检查到非此工具创建的 CL [" + current['Name'] + "] ，请确认";
                logger.error(content);
                throw new Error(content);
            }
        }
    } catch (error) {
        if (error == -34) {
            db.createCS(DATACS);
        } else {
            logger.except("检查 DATACS [" + DATACS + "] 失败", error);
            throw error;
        }
    }

    for (let i = 0; i < TOOLCLARRAY.length; i++) {
        try {
            initDataCL(TOOLCLARRAY[i]);
        } catch (error) {
            throw error;
        }
    }

    try {
        db.getCS(DATACS).getCL(TMPCLNAME).createIndex("sort",{"value":-1});
    } catch (error) {
        if (error != -247) {
            logger.except("在 [" + DATACS + "." + TMPCLNAME + "] 下创建索引失败", error);
            throw error;
        }
    }
}

function removeDataCSCL() {
    if (DROPDATACS == true) {
        for (let i = 0; i < TOOLCLARRAY.length; i++) {
            try {
                db.getCS(DATACS).dropCL(TOOLCLARRAY[i]);
            } catch (error) {
                if (error != -23) {
                    logger.except("在 DATACS [" + DATACS + "] 下删除 [" + TOOLCLARRAY[i] + "] 失败", error);
                    throw error;
                }
            }
        }

        try {
            db.dropCS(DATACS,{EnsureEmpty:true});
            //db.dropCS(DATACS);
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
    switch (MODE) {
        case "collect":
            try {
                getSnapshot();
            } catch (error) {
                throw error;
            }
            break;
        case "run":
            if (!checkAllKeyWord()) {
                break;
            }
            try {
                try {
                    initDataCS();
                } catch (error) {
                    logger.except("初始化 DATACS [" + DATACS + "] 失败", error);
                    throw error;
                }
                try {
                    importSnapshotFile();
                } catch (error) {
                    logger.except("导入快照文件到数据库中失败", error);
                    throw error;
                }
                try {
                    start();
                } catch (error) {
                    logger.except("分析模型失败", error);
                    throw error;
                }
            } catch (error) {
                throw error;
            } finally {
                try {
                    removeDataCSCL();
                } catch (error) {
                    logger.except("删除 DATACS [" + DATACS + "] 失败", error);
                    throw error;
                }
            }
            break;
        default:
            let content = "未知的 MODE: " + MODE + " ， 目前仅支持 collect 和 run 两种模式";
            logger.error(content);
            throw new Error(content);
    }
}

main();