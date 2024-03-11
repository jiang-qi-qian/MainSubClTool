// 执行为 run，回滚改为 rollback，测试为 test
if (typeof MODE == "undefined" || MODE == null || MODE == "") {
    var MODE = "test";
}

var ADDCLCONF = "conf/add_cl.csv";
var CONFIGJSON = "conf/config.json";
var CURRENTMODEL = "output/current_model.json";

var TOOL = "add_cl";
importOnce("./args.js");
importOnce("./general.js");

// 统计数据
var totalModelArray = [];
var totalCreateCS = 0;
var totalCreateCL = 0;
// 针对单独类型表的
var totalCreateCLObj = {};

var totalRollbackCS = 0;
var totalRollbackCL = 0;
var totalRollbackCLObj = {};

// 记录当前 split 组的下标
var groupIndex = 0;

var addCLFileLength;
try {
    let cmd = new Cmd();
    addCLFileLength = cmd.run("awk", "'END {print NR}' " + ADDCLCONF);
} catch (e) {
    logger.except("读取文件 [" + ADDCLCONF + "] 失败", e);
    throw e;
}

var configObj;
try {
    let cmd = new Cmd();
    configObj = JSON.parse(cmd.run("cat", CONFIGJSON));
} catch (e) {
    logger.except("读取文件 [" + CONFIGJSON + "] 失败", e);
    throw e;
}

var modelObj;
try {
    let cmd = new Cmd();
    modelObj = JSON.parse(cmd.run("cat", CURRENTMODEL));
} catch (e) {
    logger.except("读取文件 [" + CURRENTMODEL + "] 失败", e);
    throw e;
}

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

function createCL() {
    let is_first = true;
    let is_last = false;

    // 记录回滚时跳过的主表行号
    let rollbackMainCLLine = -1;
    // 记录回滚时 MAX 表的 lowbound
    let rollback_lowbound = "";
    let cmd = new Cmd();
    for (let i = 2; i <= addCLFileLength || rollbackMainCLLine != -1; i++) {
        let line;
        // 最后处理回滚的主表
        if (MODE == "rollback" && is_first && rollbackMainCLLine != -1) {
            i--;
            line = cmd.run("sed", "-n " + rollbackMainCLLine + "p " + ADDCLCONF).trim();
        } else {
            line = cmd.run("sed", "-n " + i + "p " + ADDCLCONF).trim();
        }
        let nextLine = cmd.run("sed", "-n " + (i + 1) + "p " + ADDCLCONF).trim();
        if (line.length == 0) {continue};
        if (nextLine.length == 0);
        let lineArray = line.split(',');
        let nextLineArray = nextLine.split(',');
        if (lineArray.length != 15 || (nextLineArray.length != 15 && nextLine.length != 0)) {
            let content = "新增 CL 配置文件 " + ADDCLCONF +" 格式错误，请检查";
            logger.error(content);
            throw new Error(content);
        }
        let appName = lineArray[0];
        let mainCLName = lineArray[1];
        let createCLName = lineArray[2];
        let nextMainCLName = nextLineArray[1];
        if (nextMainCLName == "" || (createCLName != nextMainCLName && mainCLName != nextMainCLName) || nextLineArray.length == 0) {
            is_last = true;
        }
        
        let type = lineArray[3];
        let domain = lineArray[4];
        let groupNum = lineArray[5];
        let shardingKey = lineArray[6];
        let lowBound = lineArray[7];
        let upBound = lineArray[8];
        let detachCL = lineArray[9];
        let pageSize = lineArray[10];
        let lobPageSize = lineArray[11];
        let partition = lineArray[12];
        let indexType = lineArray[13];
        let isMainCL = JSON.parse(lineArray[14]);

        if (-1 == totalModelArray.indexOf(appName)) {
            totalModelArray.push(appName);
        }

        // 解析 config.json 文件
        let shardingKeyType;
        let shardingKeyObj;
        let replize;
        let compressed;
        let compressionType;
        let autoIndexId;
        let ensureShardingIndex;
        let strictDataMode;
        let autoIncrement;
        let lobShardingKeyFormat;
        for (let j = 0; j < configObj.cl.length; j++) {
            let Obj = configObj.cl[j];
            if (Obj.type != type) { continue; }
            // 子表只支持 hash
            if (mainCLName != "") {
                shardingKeyType = Obj.ShardingKeyType.SubCL;
                shardingKeyObj = Obj.ShardingKey.SubCL;
                if (shardingKeyType != "hash" || shardingKeyObj == "") {
                    let content = "config.json 中 " + type + " 类型子表 ShardingKey 相关配置不正确";
                    logger.error(content);
                    throw new Error(content);
                }
            // 主表只支持 range
            } else if (isMainCL) {
                shardingKeyType = Obj.ShardingKeyType.MainCL;
                shardingKeyObj = Obj.ShardingKey.MainCL;
                if (shardingKeyType != "range" || shardingKeyObj == "") {
                    let content = "config.json 中 " + type + " 类型主表 ShardingKey 相关配置不正确";
                    logger.error(content);
                    throw new Error(content);
                }
            // 普通表认为是 LOB 表，只能是 hash
            } else {
                shardingKeyType = Obj.ShardingKeyType.NormalCL;
                shardingKeyObj = Obj.ShardingKey.NormalCL;
                if (shardingKeyType != "hash" || shardingKeyObj == "") {
                    let content = "config.json 中 " + type + " 类型普通表 ShardingKey 相关配置不正确";
                    logger.error(content);
                    throw new Error(content);
                }
            }

            replize = Obj.ReplSize;
            compressed = Obj.Compression;
            compressionType = Obj.CompressionType;
            autoIndexId = Obj.AutoIndexId;
            ensureShardingIndex = Obj.EnsureShardingIndex;
            strictDataMode = Obj.StrictDataMode;
            autoIncrement = Obj.AutoIncrement;
            if (isMainCL) {
                lobShardingKeyFormat = Obj.LobShardingKeyFormat;
                autoIndexId = "";
            } else {
                lobShardingKeyFormat = "";
            }
        }
        
        let createCS = createCLName.split('.')[0];
        let createCL = createCLName.split('.')[1];
        if (MODE == "run" || MODE == "test") {
            let real = false;
            if (MODE == "run") {
                real = true;
            }

            // 优化点
            let autoSplit;
            if (isMainCL) {
                autoSplit = false;
            } else {
                let groupArray = calaGroupArray(groupNum, domain);
                if (groupArray == "") {
                    // 全域打散
                    autoSplit = true;
                } else {
                    // 切分到一定数量的组
                    autoSplit = false;
                }
            }

            // 检查MAX分区是否有数据，没有就卸载MAX分区
            if (is_first == true && detachCL != "") {
                is_first = false;
                try {
                    let cs = detachCL.split('.')[0];
                    let cl = detachCL.split('.')[1];
                    let count = db.getCS(cs).getCL(cl).findOne().size();
                    if (count != 0) {
                        let content = "MAX 表 [" + detachCL + "] 中存在数据，请检查";
                        logger.error(content);
                        throw new Error(content);
                    }
                    detach_cl(real, db, mainCLName, detachCL);
                } catch (e) {
                    logger.except("卸载 MAX 表 [" + detachCL + "] 失败", e);
                    throw e;
                }
            }

            // 创建集合空间
            try {
                db.getCS(createCS);
                //logger.warn("the cs [" + createCS + "] is exists");
            } catch (e) {
                if (e == -34) {
                    init_cs(real, db, createCS, domain, pageSize, lobPageSize);
                } else {
                    logger.except("创建 CS [" + createCS + "] 失败", e);
                    throw e;
                }
            }
            // 创建集合
            try {
                db.getCS(createCS).getCL(createCL);
                logger.warn("集合 [" + createCS + "." + createCL + "] 已存在");
            } catch (e) {
                if (e == -23) {
                    init_cl(real, createCS, createCL, shardingKeyType, shardingKeyObj, partition, replize, compressed, compressionType, autoSplit, autoIndexId, ensureShardingIndex, strictDataMode, autoIncrement, lobShardingKeyFormat, isMainCL, type);
                } else if (e == -34 && MODE == "test") {
                    // 测试模式下 createCS 可能不存在
                    init_cl(real, createCS, createCL, shardingKeyType, shardingKeyObj, partition, replize, compressed, compressionType, autoSplit, autoIndexId, ensureShardingIndex, strictDataMode, autoIncrement, lobShardingKeyFormat, isMainCL, type);
                } else {
                    logger.except("在 CS [" + createCS + "] 上创建 CL [" + createCL + "] 失败", e);
                    throw e
                }
            }

            // 创建索引
            let indexConfigArray = get_index_info(createCS, createCL, configObj, type, indexType, mainCLName, appName, detachCL, isMainCL);
            create_index(real, createCS, createCL, indexConfigArray);

            // 挂载子表
            if (mainCLName != "") {
                attach_cl(real, mainCLName, createCLName, shardingKey, lowBound, upBound);
            }

            // 挂载 MAX 表
            if (is_last == true && detachCL != "") {
                attach_cl(real, mainCLName, detachCL, shardingKey, upBound, "MAX");
            }

            if (is_last) {logger.info("---------------------------------------------");}

            is_first = is_last;
            is_last = false;
        // 回滚
        } else if (MODE == "rollback") {
            // 跳过新建的主表，最后处理
            if (isMainCL && rollbackMainCLLine == -1) {
                rollbackMainCLLine = i;
                is_first = false;
                continue;
            } else if (isMainCL) {
                rollbackMainCLLine = -1;
            }

            // 检查MAX分区是否有数据，没有就卸载MAX分区
            if (is_first == true && detachCL != "") {
                rollback_lowbound = lowBound;
                is_first == false;
                try {
                    let cs = detachCL.split('.')[0];
                    let cl = detachCL.split('.')[1];
                    let count = db.getCS(cs).getCL(cl).findOne().size();
                    if (count != 0) {
                        let content = "表 [" + detachCL + "] 中存在数据，请检查";
                        logger.error(content);
                        throw new Error(content);
                    }
                    detach_cl(true, db, mainCLName, detachCL);
                } catch (e) {
                    logger.except("卸载子表 [" + detachCL + "] 失败", e);
                    throw e;
                }
            }

            try {
                let canDrop = false;
                // 非 LOB 表
                let dataSize = db.getCS(createCS).getCL(createCL).findOne().size();
                if (dataSize == 0) {
                    // LOB 表
                    try {
                        canDrop = true;
                        let cursor = db.exec('select Details from $SNAPSHOT_CL where Name = "' + createCS + '.' + createCL + '" and nodeselect = "primary" split by Details');
                        while (cursor.next()) {
                            if (cursor.current().toObj().Details.TotalLobs == undefined || cursor.current().toObj().Details.TotalLobs != 0){
                                canDrop = false;
                                break;
                            }
                        }
                        cursor.close();
                    } catch (e) {
                        logger.except("无法获取 CL [" + detachCL + "] 中 lob 数量", e);
                        throw e;
                    }

                }
                if (!canDrop) {
                    let content = "回滚失败，表 [" + createCLName + "] 中存在数据，请检查";
                    logger.error(content);
                    throw new Error(content);
                }
                // 卸载子表
                if (mainCLName != "") {
                    detach_cl(true, db, mainCLName, createCLName);
                }

                // 移除表
                db.getCS(createCS).dropCL(createCL);
                logger.info("删除 CL [" + createCS + "." + createCL + "] 成功");
                totalRollbackCL++;
                if (totalRollbackCLObj[type] == undefined || totalRollbackCLObj[type] == "") {
                    totalRollbackCLObj[type] = 1;
                } else {
                    totalRollbackCLObj[type]++;
                }
            } catch (e) {
                if (e == -23) {
                    logger.warn("CL [" + createCLName + "] 不存在");
                } else if (e == -34) {
                    logger.warn("CS [" + createCS + "] 不存在");
                } else {
                    logger.except("无法检查 CL [" + createCLName + "] 中是否存在数据", e);
                    throw e;
                }
            }

            // 如果集合空间为空，移除集合空间
            try {
                let size = db.getCS(createCS).listCollections().size();
                if (size == 0) {
                    // 双保险
                    db.dropCS(createCS,{EnsureEmpty:true});
                    logger.info("删除 CS [" + createCS + "] 成功");
                    totalRollbackCS++;
                }
            } catch (e) {
                if (e != -34) {
                    logger.except("删除 CS [" + createCS + "] 失败", e);
                    throw e;
                }
            }

            // 挂载 MAX 表
            if (is_last == true && detachCL != "") {
                attach_cl(true, mainCLName, detachCL, shardingKey, rollback_lowbound, "MAX");
            }
            if (is_last && rollbackMainCLLine == -1) {
                logger.info("---------------------------------------------");
            } else if (isMainCL) {
                logger.info("---------------------------------------------");
            }

            is_first = is_last;
            is_last = false;
        } else {
            let content = "未知 MODE: " + MODE;
            logger.error(content);
            throw new Error(content);
        }
    }
}

function calaGroupArray(groupNum, domain) {
    // 获取域中组的个数
    try {
        let cursor = db.getDomain(domain).listGroups();
        let domainGroup = cursor.current().toObj().Groups;
        let domainGroupNum = domainGroup.length;

        if (domainGroupNum == groupNum) {
            //return "";
        } else if (domainGroupNum < groupNum) {
            let content = "[" + ADDCLCONF + "] 配置文件中 groupNum(" + groupNum + ") 比域 [" + domain + "] 所包含的组数 (" + domainGroupNum + ") 还大，请检查";
            logger.error(content);
            throw new Error(content);
        }

        // 计算合适的组数，对 2 的 n 次方向上取整
        // 先开方，然后向上取整，再平方（没有 log2，换底实现）
        let realGroupNum = Math.pow(2, Math.ceil(Math.log(groupNum)/Math.log(2)));
        //logger.debug(groupNum + " " + realGroupNum);
        if (realGroupNum > domainGroupNum) {
            // 外层判断全域打散
            return "";
        }

        // 划分组
        let retGroupArray = [];
        domainGroup = domainGroup.sort();
        let i = groupIndex;
        //logger.debug(groupNum, realGroupNum);
        // 假设总共有 10 个组，第一个表占 8 个，算 0-7，第二个表4个，算 8-9，0-1，以此类推
        while (realGroupNum--) {
            retGroupArray.push(domainGroup[i].GroupName);
            i++;
            if (i >= domainGroupNum) {
                i = 0;
            }
        }
        groupIndex = i;
        return retGroupArray;
        //logger.debug(JSON.stringify(retGroupArray));
    } catch (e) {
        logger.except("检查域 [" + domain + "] 中组数失败", e);
        throw e;
    }
}

function get_index_info(createCS, createCL, configObj, type, indexType, mainCL, appName, withMax, isMainCL) {
    let retObjArray = [];
    let indexObj;
    //logger.debug(1);
    // 通用
    if (indexType == "general") {
        for (let j = 0; j < configObj.index.length; j++) {
            indexObj = configObj.index[j];
            if (indexObj.type != type) { continue; }
            if (isMainCL && indexObj.mainCLNeedCreate == false) {break};
            for (let k = 0; k < indexObj.index.length; k++) {
                let indexConf = indexObj.index[k];
                let tmpObj = {};
                tmpObj["indexName"] = indexConf.name;
                tmpObj["indexDef"] = JSON.parse(indexConf.def);
                tmpObj["indexUnique"] = indexConf.unique;
                tmpObj["indexEnforced"] = indexConf.enforced;
                tmpObj["indexNotArray"] = indexConf.notarray;
                tmpObj["indexNotNull"] = indexConf.notnull;
                retObjArray.push(tmpObj);
            }
        }
    // 继承
    } else if (indexType == "inherit") {
        // 获取上一个时间分区表的索引
        let lastSubCLName;
        try {
            if (mainCL != "") {
                // 主子表
                let size = db.snapshot(8,{Name:mainCL, IsMainCL:true}).current().toObj()['CataInfo'].length;
                let cursor = db.exec('select CataInfo from $SNAPSHOT_CATA where IsMainCL=true and Name="' + mainCL + '"');
                let CataInfo = cursor.current().toObj()['CataInfo'];
                // 跳过 MAX 表往前
                if (withMax != "") {
                    lastSubCLName = CataInfo[size - 2].SubCLName;
                } else {
                    lastSubCLName = CataInfo[size - 1].SubCLName;
                }
                cursor.close();
            } else {
                // 普通表
                //logger.debug(JSON.stringify(modelObj.normal_cl));
                let model = "";
                for (let j = 0; j < modelObj.normal_cl.length; j++) {
                    let nromalCLObj = modelObj.normal_cl[j];
                    // logger.info(nromalCLObj.type + " " + type);
                    // 需要找到普通表上一张表名
                    if (nromalCLObj.type != type) { continue; }
                    model = nromalCLObj.model;
                    break;
                }
                model = model.replace(new RegExp("\\$APPNAME", "g"), appName);
                // logger.debug(model);
                let condArray = changDate(model);
                let fullCLName = createCS + "." + createCL;
                for (let j = 0; j < condArray.length; j++) {
                    if (fullCLName.replace(new RegExp(condArray[j]), "") == "") {
                        let cursor = db.list(4,{Name: {"$regex": condArray[j]}},{Name:1});
                        let CLNameArray = [];
                        while (cursor.next()) {
                            CLNameArray.push(cursor.current().toObj()['Name']);
                        }
                        cursor.close();
                        CLNameArray.sort();
                        lastSubCLName = CLNameArray[CLNameArray.length - 1];
                        break;
                    }
                }
            }
        } catch (e) {
            logger.except("无法获取上一个时间分区表的信息", e);
            throw e
        }
        let cs_name = lastSubCLName.split('.')[0];
        let cl_name = lastSubCLName.split('.')[1];
        try {
            let cursor = db.getCS(cs_name).getCL(cl_name).listIndexes();
            let indexArray = cursor.toArray();
            for (let i = 0; i < indexArray.length; i++){
                let conf = JSON.parse(indexArray[i]);
                let indexConf = conf.IndexDef;
                if (indexConf.name == "$id" || indexConf.name == "$shard") {continue};
                let tmpObj = {};
                tmpObj["indexName"] = indexConf.name;
                tmpObj["indexDef"] = indexConf.key;
                tmpObj["indexUnique"] = indexConf.unique;
                tmpObj["indexEnforced"] = indexConf.enforced;
                tmpObj["indexNotArray"] = indexConf.NotArray;
                tmpObj["indexNotNull"] = indexConf.NotNull;
                retObjArray.push(tmpObj);
            }
        } catch (e) {
            logger.except("无法从 CL [" + lastSubCLName + "] 中获取索引信息", e);
            throw e
        }
    } else {
        let content = "未知 indexType [" + indexType + "]";
        logger.error(content);
        throw new Error(content);
    }
    return retObjArray;
}

function create_index(real, createCS, createCL, configObjArray) {
    for (let i = 0; i < configObjArray.length; i++) {
        let configObj = configObjArray[i];
        let indexName = configObj.indexName;
        let indexDef = configObj.indexDef;
        let indexUnique = configObj.indexUnique;
        let indexEnforced = configObj.indexEnforced;
        let indexNotArray = configObj.indexNotArray;
        let indexNotNull = configObj.indexNotNull;
        try {
            let cmd = "db.getCS(\"" + createCS + "\").getCL(\"" + createCL + "\").createIndex(\"" + indexName + "\"," + JSON.stringify(indexDef) + ",{Unique:" + indexUnique + ",Enforced:" + indexEnforced + ",NotNull:" + indexNotNull + ",NotArray:" + indexNotArray + "});";
            if (real) {
                eval(cmd);
                logger.info("在 CL [" + createCS + "." + createCL + "] 上创建索引 [" + indexName + "] 成功");
            } else {
                logger.info(cmd);
            }
        } catch (error) {
            if (error == -247) {
                logger.warn("索引 [" + indexName + "] 已经创建在 [" + createCS + "." + createCL + "] 上");
            } else {
                logger.except("在 CL [" + createCS + "." + createCL + "] 上创建索引 [" + indexName + "] 失败", error);
                throw error;
            }
        }
    }
}

function detach_cl(real, db, maincl, subcl) {
    try {
        let maincs_name = maincl.split('.')[0];
        let maincl_name = maincl.split('.')[1];
        let cs = subcl.split('.')[0];
        let cl = subcl.split('.')[1];
        let count = db.getCS(cs).getCL(cl).findOne().size();
        if (count != 0) {
            let content = "无法卸载表，表 [" + subcl + "] 中存在数据，请检查";
            logger.error(content);
            throw new Error(content);
        }
        let cmd = "db.getCS(\"" + maincs_name + "\").getCL(\"" + maincl_name + "\").detachCL(\"" + subcl + "\")";
        if (real) {
            eval(cmd);
            logger.info("在 [" + maincl + "] 上卸载 [" + subcl + "] 成功");
        } else {
            logger.info(cmd);
        }
    } catch (e) {
        if (e == -242) {
            logger.warn("CL [" + subcl + "] 没有挂载在 [" + maincl + "] 上");
        }else if (e == -23) {
            logger.warn("CL [" + subcl + "] 不存在");
        } else {
            logger.except("在 [" + maincl + "] 上卸载 [" + subcl + "] 失败", e);
            throw e;
        }
    }
}

function attach_cl(real, maincl, subcl, shardingKey, lowBound, upBound) {
    let maincs_name = maincl.split('.')[0];
    let maincl_name = maincl.split('.')[1];

    try {
        // 检查是否已挂载
        var cursor = db.exec('select MainCLName from $SNAPSHOT_CATA where Name="' + subcl + '"');
        try {
            if (cursor.current().toObj().MainCLName != "" && cursor.current().toObj().MainCLName != undefined) {
                if (!real && upBound == "MAX") {
                    // 跳过，正常来说要求未挂载，test 模式比较特殊，因为前面没有卸载 MAX 表，所以这里要求 MAX 表已挂载
                } else {
                    logger.warn("CL [" + subcl + "] 已经挂载到 [" + maincl + "] 上");
                    return;
                }
            } else {
                if (!real && upBound == "MAX") {
                    logger.warn("CL [" + subcl + "] 没有挂载到 [" + maincl + "]");
                }
            }
        } catch (e) {
            if (e != -29) {
                logger.except("获取 CL [" + subcl + "] 的 $SNAPSHOT_CATA 信息失败", e);
                throw e;
            }
        }

        let cmd;
        if (upBound == "MAX") {
            cmd = "db.getCS(\"" + maincs_name + "\").getCL(\"" + maincl_name + "\").attachCL(\"" + subcl + "\",{LowBound:{" + shardingKey + ":\"" + lowBound + "\"},UpBound:{" + shardingKey + ":MaxKey()}});"
        } else {
            cmd = "db.getCS(\"" + maincs_name + "\").getCL(\"" + maincl_name + "\").attachCL(\"" + subcl + "\",{LowBound:{" + shardingKey + ":\"" + lowBound + "\"},UpBound:{" + shardingKey + ":\"" + upBound + "\"}});"
        }

        if (real) {
            eval(cmd);
            logger.info("挂载 CL [" + subcl + "] 到 [" + maincl+ "] 成功, shardingKey [" + shardingKey + "], lowBound [" + lowBound + "], upBound [" + upBound + "]");
        } else {
            //logger.info("'var db=new Sdb(\""+COORDADDR+"\",\""+COORDSVC+"\",\""+DBUSER+"\",\""+DBPASSWORD+"\");db.getCS(\""+maincs_name+"\").getCL(\""+maincl_name+"\").attachCL(\""+subcl+"\",{LowBound:{"+shardingKey+":\""+lowBound+"\"},UpBound:{"+shardingKey+":\""+upBound+"\"}});'");
            logger.info(cmd);
        }
    } catch(error) {
        logger.except("挂载 CL [" + subcl + "] 到 [" + maincl + "] 失败", error);
        throw error
    }
}

function init_cs(real, db, cs_name, lobDomainName, pageSize, lobPageSize) {
    try {
        let cmd = "db.createCS(\"" + cs_name + "\",{Domain:\"" + lobDomainName + "\",PageSize:" + parseInt(pageSize, 10) + ",LobPageSize:" + lobPageSize + "})";
        if (real) {
            eval(cmd);
            logger.info("创建 CS [" + cs_name + "] 成功");
        } else {
            logger.info(cmd);
        }
        totalCreateCS++;
    } catch (error) {
        logger.except("创建 CS [" + cs_name + "] 失败", error);
        throw error
    }
}

function init_cl(real, cs_name, cl_name, shardingType, shardingKeyObj, partition, replSize, compressed, compressionType, autoSplit, autoIndexId, ensureShardingIndex, strictDataMode, autoIncrement, lobShardingKeyFormat, isMainCL, type, groupNum) {
    try {
        let cmd_prefix = "db.getCS(\""+cs_name+"\").createCL(\""+cl_name+"\", { ShardingType: \""+shardingType+"\", ShardingKey: "+JSON.stringify(shardingKeyObj)+", ReplSize: "+replSize+", AutoSplit: "+autoSplit+", EnsureShardingIndex: "+ensureShardingIndex+", StrictDataMode: "+strictDataMode+", IsMainCL: "+isMainCL;
        let cmd_suffix = "});";
        let cmd_mid = "";
        if (compressed) {
            cmd_mid += ", Compressed: "+compressed + ", CompressionType: " + compressionType;
        }
        if (autoIncrement != "") {
            cmd_mid += ", AutoIncrement: " + autoIncrement;
        }
        if (lobShardingKeyFormat != "") {
            cmd_mid += ", LobShardingKeyFormat: \"" + lobShardingKeyFormat + "\"";
        }
        if (shardingType == "hash") {
            cmd_mid += ", Partition: " + partition;
        }
        if (autoIndexId != "") {
            cmd_mid += ", AutoIndexId: "+ autoIndexId;
        }

        let cmd = cmd_prefix + cmd_mid + cmd_suffix;
        if (real) {
            eval(cmd);
            logger.info("在 CS [" + cs_name + "] 上创建 CL [" + cl_name + "] 成功");
        } else {
            //logger.info(real+" "+ db+" "+ cs_name+" "+ cl_name+" "+ shardingType+" "+ JSON.stringify(shardingKeyObj)+" "+ partition+" "+ replSize+" "+ compressed+" "+ compressionType+" "+ autoSplit+" "+ autoIndexId+" "+ ensureShardingIndex+" "+ strictDataMode+" "+ autoIncrement+" "+ lobShardingKeyFormat+" "+ isMainCL);
            //logger.info("db.getCS("+cs_name+").createCL("+cl_name+", { ShardingType: "+shardingType+", ShardingKey: "+JSON.stringify(shardingKeyObj)+", Partition: "+parseInt(partition, 10)+", ReplSize: "+replSize+", Compressed: "+compressed+", CompressionType: "+compressionType+", AutoSplit: "+autoSplit+", AutoIndexId: "+autoIndexId+", EnsureShardingIndex: "+ensureShardingIndex+", StrictDataMode: "+strictDataMode+", AutoIncrement: "+autoIncrement+", LobShardingKeyFormat: "+lobShardingKeyFormat+", IsMainCL: "+isMainCL+"});");
            logger.info(cmd);
        }
        // 收集统计信息
        totalCreateCL++;
        if (totalCreateCLObj[type] == undefined || totalCreateCLObj[type] == "") {
            totalCreateCLObj[type] = 1;
        } else {
            totalCreateCLObj[type]++;
        }
    } catch (error) {
        logger.except("在 CS [" + cs_name + "] 上创建 CL [" + cl_name + "] 失败", error);
        throw error
    }
}

function printlnInfo(flag) {
    if (flag) {
        if (MODE == "run" || MODE == "test") {
            logger.info("总 APP 数为: " + totalModelArray.length);
            logger.info("总创建 CS 数为: " + totalCreateCS);
            logger.info("总创建 CL 数为: " + totalCreateCL);
            for (let key in totalCreateCLObj) {
                logger.info(key + " 类型的 CL 数为: " + totalCreateCLObj[key]);
            }
        } else if (MODE == "rollback") {
            logger.info("总 APP 数为: " + totalModelArray.length);
            logger.info("总回滚 CS 数为: " + totalRollbackCS);
            logger.info("总回滚 CL 数为: " + totalRollbackCL);
            for (let key in totalRollbackCLObj) {
                logger.info(key + " 类型的 CL 数为: " + totalRollbackCLObj[key]);
            }
        } else {
            let content = "Unknown mode: " + MODE;
            logger.error(content);
            throw new Error(content);
        }
    } else {
        if (MODE == "run" || MODE == "test") {
            logger.info("当前 APP 数为: " + totalModelArray.length);
            logger.info("当前创建 CS 数为: " + totalCreateCS);
            logger.info("当前创建 CL 数为: " + totalCreateCL);
            for (let key in totalCreateCLObj) {
                logger.info(key + " 类型的 CL 数为: " + totalCreateCLObj[key]);
            }
        } else if (MODE == "rollback") {
            logger.info("当前 APP 数为: " + totalModelArray.length);
            logger.info("当前回滚 CS 数为: " + totalRollbackCS);
            logger.info("当前回滚 CL 数为: " + totalRollbackCL);
            for (let key in totalRollbackCLObj) {
                logger.info(key + " 类型的 CL 数为: " + totalRollbackCLObj[key]);
            }
        } else {
            let content = "Unknown mode: " + MODE;
            logger.error(content);
            throw new Error(content);
        }
    }
}

/*
    start
*/
function main() {
    try {
        createCL();
    } catch (error) {
        printlnInfo(false);
        throw error;
    }
    printlnInfo(true);
}

main();