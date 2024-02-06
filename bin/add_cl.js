// 执行为 run，回滚改为 rollback，测试为 test
if (typeof MODE == "undefined" || MODE == null || MODE == "") {
    var MODE = "test";
}

var ADDCLCONF = "conf/add_cl.csv";
var CONFIGJSON = "conf/config.json"

var TOOL = "add_cl";
importOnce("./args.js");
importOnce("./general.js");

function createCL() {
    let length;
    let cmd = new Cmd();
    try {
        length = cmd.run("awk", "'END {print NR}' " + ADDCLCONF);
    } catch (error) {
        logger.except("failed to read file [" + ADDCLCONF + "]", e);
        throw e;
    }

    let configObj;
    try {
        configObj = JSON.parse(cmd.run("cat", CONFIGJSON));
    } catch (error) {
        logger.except("failed to read file [" + CONFIGJSON + "]", e);
        throw e;
    }

    let is_first = true;
    let is_last = false;

    // 记录回滚时 MAX 表的 lowbound
    let rollback_lowbound = "";
    for (let i = 2; i <= length; i++) {
        let line = cmd.run("sed", "-n " + i + "p " + ADDCLCONF).trim();
        let nextLine = cmd.run("sed", "-n " + (i + 1) + "p " + ADDCLCONF).trim();
        if (line.length == 0) {continue};
        if (nextLine.length == 0);
        let lineArray = line.split(',');
        let nextLineArray = nextLine.split(',');
        if (lineArray.length != 13 || (nextLineArray.length != 13 && nextLine.length != 0)) {
            let content = "域配置文件 " + ADDCLCONF +" 格式错误，请检查";
            logger.error(content);
            throw new Error(content);
        }
        let appName = lineArray[0];
        let nextAppName = nextLineArray[0];
        let mainCLName = lineArray[1];
        let nextMainCLName = nextLineArray[1];
        if (mainCLName != nextMainCLName || nextLineArray.length == 0) {
            is_last = true;
        }

        let createCLName = lineArray[2];
        let CLType = lineArray[3];
        let domain = lineArray[4];
        let shardingKey = lineArray[5];
        let lowBound = lineArray[6];
        let upBound = lineArray[7];
        let detachCL = lineArray[8];
        let pageSize = lineArray[9];
        let lobPageSize = lineArray[10];
        let partition = lineArray[11];
        let indexType = lineArray[12];

        // 优化点
        let autoSplit = true;

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
            if (Obj.type != CLType) { continue; }
            // 目前只支持 hash
            if (mainCLName != "") {
                shardingKeyType = Obj.ShardingKeyType.SubCL;
                shardingKeyObj = Obj.ShardingKey.SubCL;
                if (shardingKeyType != "hash" || shardingKeyObj == "") {
                    let content = "config.json 中 " + CLType + " 类型子表 ShardingKey 相关配置不正确";
                    logger.error(content);
                    throw new Error(content);
                }
            } else {
                // 通常是 LOB 普通表
                shardingKeyType = Obj.ShardingKeyType.NormalCL;
                shardingKeyObj = Obj.ShardingKey.NormalCL;
                if (shardingKeyType != "hash" || shardingKeyObj == "") {
                    let content = "config.json 中 " + CLType + " 类型普通表 ShardingKey 相关配置不正确";
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
            lobShardingKeyFormat = Obj.LobShardingKeyFormat;
        }
        
        let createCS = createCLName.split('.')[0];
        let createCL = createCLName.split('.')[1];
        if (MODE == "run" || MODE == "test") {
            let real = false;
            if (MODE == "run") {
                real = true;
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
                    logger.except("failed to find detach cl [" + detachCL + "]", e);
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
                    logger.except("failed to create cs [" + createCS + "]", e);
                    throw e;
                }
            }
            // 创建集合
            try {
                db.getCS(createCS).getCL(createCL);
                logger.warn("the collection [" + createCS + "." + createCL + "] is exists");
            } catch (e) {
                if (e == -23) {
                    // 扩展表不关心主表
                    let isMainCL = false;
                    let lobShardingKeyFormat = "";
                    init_cl(real, db, createCS, createCL, shardingKeyType, shardingKeyObj, partition, replize, compressed, compressionType, autoSplit, autoIndexId, ensureShardingIndex, strictDataMode, autoIncrement, lobShardingKeyFormat, isMainCL);
                } else if (e == -34 && MODE == "test") {
                    // 测试模式下 createCS 可能不存在
                    let isMainCL = false;
                    let lobShardingKeyFormat = "";
                    init_cl(real, db, createCS, createCL, shardingKeyType, shardingKeyObj, partition, replize, compressed, compressionType, autoSplit, autoIndexId, ensureShardingIndex, strictDataMode, autoIncrement, lobShardingKeyFormat, isMainCL);
                } else {
                    logger.except("failed to the cs [" + createCS + "] create cl [" + createCL + "]", e);
                    throw e
                }
            }

            // 创建索引
            let needCreateIndex = false;
            try {
                if (real) {
                    let size = db.getCS(createCS).getCL(createCL).listIndexes().size();
                    //logger.info("size "+size);
                    if (size == undefined || size == 0) {
                        needCreateIndex = true;
                    } else if (size == 1) {
                        let cursor = db.getCS(createCS).getCL(createCL).listIndexes();
                        let conf = JSON.parse(cursor.toArray()[0]);
                        if (conf.IndexDef.name == "$id") {
                            needCreateIndex = true;
                        };
                    }
                }
            } catch (e) {
                logger.except("failed to check index from [" + createCS + "." + "createCL" + "]", e);
                throw e
            }

            if (needCreateIndex || real == false) {
                create_index(real, createCS, createCL, configObj, CLType, indexType, mainCLName);
            }

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
                    logger.except("failed to find detach cl [" + detachCL + "]", e);
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
                        logger.except("failed to get cl [" + detachCL + "] lobs count", e);
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
                logger.info("drop cl [" + createCS + "." + createCL + "] success");
            } catch (e) {
                if (e == -23) {
                    logger.warn("cl [" + createCLName + "] does not exist");
                } else if (e == -34) {
                    logger.warn("cs [" + createCS + "] does not exist");
                } else {
                    logger.except("failed to check cl [" + createCLName + "] data", e);
                    throw e;
                }
            }

            // 如果集合空间为空，移除集合空间
            try {
                let size = db.getCS(createCS).listCollections().size();
                if (size == 0) {
                    // 双保险
                    db.dropCS(createCS,{EnsureEmpty:true});
                    logger.info("drop cs [" + createCS + "] success");
                }
            } catch (e) {
                if (e != -34) {
                    logger.except("failed to drop cs [" + createCS + "]", e);
                    throw e;
                }
            }

            // 挂载 MAX 表
            if (is_last == true && detachCL != "") {
                attach_cl(true, mainCLName, detachCL, shardingKey, rollback_lowbound, "MAX");
            }

            if (is_last) {logger.info("---------------------------------------------");}

            is_first = is_last;
            is_last = false;
        } else {
            let content = "Unknown mode: " + MODE;
            logger.error(content);
            throw new Error(content);
        }
    }
}

function create_index(real, createCS, createCL, configObj, CLType, indexType, mainCL) {
    let indexObj;
    let indexName;
    let indexDef;
    let indexUnique;
    let indexEnforced;
    let indexNotArray;
    let indexNotNull;
    if (indexType == "general") {
        for (let j = 0; j < configObj.index.length; j++) {
            indexObj = configObj.index[j];
            if (indexObj.type != CLType) { continue; }
            for (let k = 0; k < indexObj.index.length; k++) {
                let indexConf = indexObj.index[k];
                indexName = indexConf.name;
                indexDef = JSON.parse(indexConf.def);
                indexUnique = indexConf.unique;
                indexEnforced = indexConf.enforced;
                indexNotArray = indexConf.notarray;
                indexNotNull = indexConf.notnull;

                try {
                    if (real) {
                        db.getCS(createCS).getCL(createCL).createIndex(indexName, indexDef, {Unique: indexUnique, Enforced: indexEnforced, NotNull: indexNotNull, NotArray: indexNotArray});
                        logger.info("create index [" + indexName + "] on [" + createCS + "." + createCL + "] success");
                    } else {
                        logger.info("db.getCS("+createCS+").getCL("+createCL+").createIndex("+indexName+", "+JSON.stringify(indexDef)+", {Unique: "+indexUnique+", Enforced: "+indexEnforced+", NotNull: "+indexNotNull+", NotArray: "+indexNotArray+"});");
                    }
                } catch (error) {
                    logger.except("failed to create index [" + indexName + "] on [" + createCS + "." + createCL + "]", error);
                    throw error
                }
            }
        }
    // 继承
    } else if (indexType == "inherit") {
        // 获取主表最后一张表的索引
        let lastSubCLName;
        try {
            var cursor = db.exec('select CataInfo from $SNAPSHOT_CATA where IsMainCL=true and Name="' + mainCL + '"');
            var CataInfo = cursor.current().toObj()['CataInfo'];
            lastSubCLName = CataInfo[0].SubCLName;
            cursor.close();
        } catch (e) {
            logger.except("failed to get last subCL from [" + mainCL + "]", e);
            throw e
        }

        let cs_name = lastSubCLName.split('.')[0];
        let cl_name = lastSubCLName.split('.')[1];
        try {
            let cursor = db.getCS(cs_name).getCL(cl_name).listIndexes();
            let indexArray = cursor.toArray();
            //logger.info(indexArray);
            //logger.info(lastSubCLName + " " + indexArray.length);
            for (let i = 0; i < indexArray.length; i++){
                let conf = JSON.parse(indexArray[i]);
                let indexConf = conf.IndexDef;
                if (indexConf.name == "$id" || indexConf.name == "$shard") {continue};
    
                indexName = indexConf.name;
                indexDef = indexConf.key;
                indexUnique = indexConf.unique;
                indexEnforced = indexConf.enforced;
                indexNotArray = indexConf.NotArray;
                indexNotNull = indexConf.NotNull;

                try {
                    if (real) {
                        db.getCS(createCS).getCL(createCL).createIndex(indexName, indexDef, {Unique: indexUnique, Enforced: indexEnforced, NotNull: indexNotNull, NotArray: indexNotArray});
                        logger.info("create index [" + indexName + "] on [" + createCS + "." + createCL + "] success");
                    } else {
                        logger.info("db.getCS("+createCS+").getCL("+createCL+").createIndex("+indexName+", "+JSON.stringify(indexDef)+", {Unique: "+indexUnique+", Enforced: "+indexEnforced+", NotNull: "+indexNotNull+", NotArray: "+indexNotArray+"});");
                    }
                } catch (e) {
                    logger.except("failed to create index [" + indexName + "] on [" + createCS + "." + createCL + "]", e);
                    throw e
                }
            }
        } catch (e) {
            logger.except("failed to get index from [" + lastSubCLName + "]", e);
            throw e
        }
    } else {
        let content = "Unknown indexType [" + indexType + "]";
        logger.error(content);
        throw new Error(content);
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
        if (real) {
            db.getCS(maincs_name).getCL(maincl_name).detachCL(subcl);
            logger.info("detach cl [" + subcl + "] without [" + maincl + "] success");
        } else {
            logger.info("db.getCS("+maincs_name+").getCL("+maincl_name+").detachCL("+subcl+");");
        }
    } catch (e) {
        if (e == -242) {
            logger.warn("[" + subcl + "] does not attach on [" + maincl + "]");
        }else if (e == -23) {
            logger.warn("[" + subcl + "] does not exist");
        } else {
            logger.except("failed to detach cl [" + subcl + "] without [" + maincl + "]", e);
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
                logger.warn("subcl [" + subcl + "] has been attach to [" + maincl + "]");
                return;
            }
        } catch (e) {
            if (e != -29) {
                logger.except("failed to get $SNAPSHOT_CATA from  [" + subcl + "]", e);
                throw e;
            }
        }

        let cmd;
        if (upBound == "MAX") {
            cmd = "db.getCS(\""+maincs_name+"\").getCL(\""+maincl_name+"\").attachCL(\""+subcl+"\",{LowBound:{"+shardingKey+":\""+lowBound+"\"},UpBound:{"+shardingKey+":MaxKey()}});"
        } else {
            cmd = "db.getCS(\""+maincs_name+"\").getCL(\""+maincl_name+"\").attachCL(\""+subcl+"\",{LowBound:{"+shardingKey+":\""+lowBound+"\"},UpBound:{"+shardingKey+":\""+upBound+"\"}});"
        }

        if (real) {
            eval(cmd);
            logger.info("attach cl [" + subcl + "] to [" + maincl+ "] success, shardingKey is [" + shardingKey + "], lowBound is [" + lowBound + "], upBound is [" + upBound + "]");
        } else {
            //logger.info("'var db=new Sdb(\""+COORDADDR+"\",\""+COORDSVC+"\",\""+DBUSER+"\",\""+DBPASSWORD+"\");db.getCS(\""+maincs_name+"\").getCL(\""+maincl_name+"\").attachCL(\""+subcl+"\",{LowBound:{"+shardingKey+":\""+lowBound+"\"},UpBound:{"+shardingKey+":\""+upBound+"\"}});'");
            logger.info(cmd);
        }
    } catch(error) {
        logger.except("failed to attch cl [" + subcl + "] to [" + maincl + "]", error);
        throw error
    }
}

function init_cs(real, db, cs_name, lobDomainName, pageSize, lobPageSize) {
    try {
        //cmd = logger.info("db.createCS("+cs_name+",{Domain:"+lobDomainName+",PageSize:"+pageSize+",LobPageSize:"+lobPageSize+"})");
        if (real) {
            //eval(cmd);
            db.createCS(cs_name, { Domain: lobDomainName, PageSize: parseInt(pageSize,10), LobPageSize: parseInt(lobPageSize,10) });
            logger.info("create cs [" + cs_name + "] success");
        } else {
            //print cmd
            logger.info("db.createCS("+cs_name+",{Domain:"+lobDomainName+",PageSize:"+pageSize+",LobPageSize:"+lobPageSize+"})");
        }
    } catch (error) {
        logger.except("failed to create cs [" + cs_name + "] with Domain:"+lobDomainName+",LobPageSize:"+lobPageSize+"", error);
        throw error
    }
}

function init_cl(real, db, cs_name, cl_name, shardingType, shardingKeyObj, partition, replSize, compressed, compressionType, autoSplit, autoIndexId, ensureShardingIndex, strictDataMode, autoIncrement, lobShardingKeyFormat, isMainCL) {
    try {
        if (real) {
            if (compressed) {
                if (autoIncrement == "") {
                    if (lobShardingKeyFormat == "") {
                        db.getCS(cs_name).createCL(cl_name, { ShardingType: shardingType, ShardingKey: shardingKeyObj, Partition: parseInt(partition, 10), ReplSize: replSize, Compressed: compressed, CompressionType: compressionType, AutoSplit: autoSplit, AutoIndexId: autoIndexId, EnsureShardingIndex: ensureShardingIndex, StrictDataMode: strictDataMode, IsMainCL: isMainCL});
                    } else {
                        db.getCS(cs_name).createCL(cl_name, { ShardingType: shardingType, ShardingKey: shardingKeyObj, Partition: parseInt(partition, 10), ReplSize: replSize, Compressed: compressed, CompressionType: compressionType, AutoSplit: autoSplit, AutoIndexId: autoIndexId, EnsureShardingIndex: ensureShardingIndex, StrictDataMode: strictDataMode, LobShardingKeyFormat: lobShardingKeyFormat, IsMainCL: isMainCL});
                    }
                } else {
                    if (lobShardingKeyFormat == "") {
                        db.getCS(cs_name).createCL(cl_name, { ShardingType: shardingType, ShardingKey: shardingKeyObj, Partition: parseInt(partition, 10), ReplSize: replSize, Compressed: compressed, CompressionType: compressionType, AutoSplit: autoSplit, AutoIndexId: autoIndexId, EnsureShardingIndex: ensureShardingIndex, StrictDataMode: strictDataMode, AutoIncrement: autoIncrement, IsMainCL: isMainCL});
                    } else {
                        db.getCS(cs_name).createCL(cl_name, { ShardingType: shardingType, ShardingKey: shardingKeyObj, Partition: parseInt(partition, 10), ReplSize: replSize, Compressed: compressed, CompressionType: compressionType, AutoSplit: autoSplit, AutoIndexId: autoIndexId, EnsureShardingIndex: ensureShardingIndex, StrictDataMode: strictDataMode, AutoIncrement: autoIncrement, LobShardingKeyFormat: lobShardingKeyFormat, IsMainCL: isMainCL});
                    }
                }
            } else {
                if (autoIncrement == "") {
                    if (lobShardingKeyFormat == "") {
                        db.getCS(cs_name).createCL(cl_name, { ShardingType: shardingType, ShardingKey: shardingKeyObj, Partition: parseInt(partition, 10), ReplSize: replSize, Compressed: compressed, AutoSplit: autoSplit, AutoIndexId: autoIndexId, EnsureShardingIndex: ensureShardingIndex, StrictDataMode: strictDataMode, IsMainCL: isMainCL});
                    } else {
                        db.getCS(cs_name).createCL(cl_name, { ShardingType: shardingType, ShardingKey: shardingKeyObj, Partition: parseInt(partition, 10), ReplSize: replSize, Compressed: compressed, AutoSplit: autoSplit, AutoIndexId: autoIndexId, EnsureShardingIndex: ensureShardingIndex, StrictDataMode: strictDataMode, LobShardingKeyFormat: lobShardingKeyFormat, IsMainCL: isMainCL});
                    }
                } else {
                    if (lobShardingKeyFormat == "") {
                        db.getCS(cs_name).createCL(cl_name, { ShardingType: shardingType, ShardingKey: shardingKeyObj, Partition: parseInt(partition, 10), ReplSize: replSize, Compressed: compressed, AutoSplit: autoSplit, AutoIndexId: autoIndexId, EnsureShardingIndex: ensureShardingIndex, StrictDataMode: strictDataMode, AutoIncrement: autoIncrement, IsMainCL: isMainCL});
                    } else {
                        db.getCS(cs_name).createCL(cl_name, { ShardingType: shardingType, ShardingKey: shardingKeyObj, Partition: parseInt(partition, 10), ReplSize: replSize, Compressed: compressed, AutoSplit: autoSplit, AutoIndexId: autoIndexId, EnsureShardingIndex: ensureShardingIndex, StrictDataMode: strictDataMode, AutoIncrement: autoIncrement, LobShardingKeyFormat: lobShardingKeyFormat, IsMainCL: isMainCL});
                    }
                }
            }
            logger.info("the cs [" + cs_name + "] create cl [" + cl_name + "] success");
        } else {
            //logger.info(real+" "+ db+" "+ cs_name+" "+ cl_name+" "+ shardingType+" "+ JSON.stringify(shardingKeyObj)+" "+ partition+" "+ replSize+" "+ compressed+" "+ compressionType+" "+ autoSplit+" "+ autoIndexId+" "+ ensureShardingIndex+" "+ strictDataMode+" "+ autoIncrement+" "+ lobShardingKeyFormat+" "+ isMainCL);
            logger.info("db.getCS("+cs_name+").createCL("+cl_name+", { ShardingType: "+shardingType+", ShardingKey: "+JSON.stringify(shardingKeyObj)+", Partition: "+parseInt(partition, 10)+", ReplSize: "+replSize+", Compressed: "+compressed+", CompressionType: "+compressionType+", AutoSplit: "+autoSplit+", AutoIndexId: "+autoIndexId+", EnsureShardingIndex: "+ensureShardingIndex+", StrictDataMode: "+strictDataMode+", AutoIncrement: "+autoIncrement+", LobShardingKeyFormat: "+lobShardingKeyFormat+", IsMainCL: "+isMainCL+"});");
            logger.info("create cl [" + cs_name + "." + cl_name + "]");
        }
    } catch (error) {
        logger.except("failed to the cs [" + cs_name + "] create cl [" + cl_name + "]", error);
        throw error
    }
}

/*
    start
*/

function main() {
    createCL();
}

main();