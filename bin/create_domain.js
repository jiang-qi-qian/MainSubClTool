// 执行为 run，回滚改为 rollback
if (typeof MODE == "undefined" || MODE == null || MODE == "") {
    var MODE = "test";
}

var DOMIANCSV = "conf/domain.csv";
var TOOL = "create_domain";
importOnce("./args.js");
importOnce("./general.js");

function checkParam(domainName, groups) {
    if (typeof domainName == "undefined" || domainName == null || domainName == "") {
        let content = "启动参数 [domain_name] 有问题，请检查";
        logger.error(content);
        throw new Error(content);
    }
    if (typeof groups == "undefined" || groups == null) {
        let content = "启动参数 [groups] 有问题，请检查";
        logger.error(content);
        throw new Error(content);
    }
}

function createDomain() {
    let cmd = new Cmd();
    let domainArray = JSON.parse(cmd.run("cat", DOMIANCSV));
    if (!Array.isArray(domainArray)) {
        let content = "域配置文件 conf/domain.csv 格式错误，请检查";
        logger.error(content);
        throw new Error(content);
    }
    for (let i = 0; i < domainArray.length; i++) {
        let domainName = domainArray[i].domainName;
        let groupArray = domainArray[i].groups;
        checkParam(domainName, groupArray);
        // 执行
        if (MODE == "run") {
            try {
                db.getDomain(domainName);
                logger.warn("域 [" + domainName + "(" + groupArray + ")] 已存在");
                continue;
            } catch (e) {
                if (-214 == e) {
                    try {
                        db.createDomain(domainName, groupArray, { AutoSplit: true });
                        logger.info("创建域 [" + domainName + "(" + groupArray + ")] 成功");
                    } catch (error) {
                        logger.except("创建域 [" + domainName + "(" + groupArray + ")] 失败", error);
                        throw error;
                    }
                } else {
                    logger.except("获取域 [" + domainName + "] 信息失败", e);
                    throw e;
                }
            }
        // 回滚
        } else if (MODE == "rollback") {
            try {
                let cs_num = db.getDomain(domainName).listCollectionSpaces().size();
                if (cs_num != 0){
                    logger.error("无法删除域 [" + domainName + "]，因为域中还存在 CS");
                    continue;
                }
                try {
                    db.dropDomain(domainName);
                    logger.info("删除域 [" + domainName + "] 成功");
                } catch (error) {
                    logger.except("删除域 [" + domainName + "] 失败", error);
                    throw error;
                }
            } catch (e) {
                if (-214 == e) {
                    // 忽略
                    // logger.warn("域 [" + domainName + "(" + groupArray + ")] 不存在");
                } else {
                    logger.except("无法检查域 [" + domainName + "] 是否存在", e);
                    throw e;
                }
            }
        // 测试
        } else if (MODE == "test") {
            logger.info("域名: " + domainName + "，组: (" + groupArray + ")");
        } else {
            let content = "未知的 MODE: " + MODE + " ， 目前仅支持 test, run 和 rollback 三种模式";
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
        createDir('log');
        createDir('output');
    } catch (error) {
        let content = "无法在当前目录下创建目录 log/ 和 output/";
        logger.error(content);
        throw new Error(content);
    }

    try {
        openLog();
    } catch (error) {
        let content = "无法在打开日志文件 " + log_file;
        logger.error(content);
        throw new Error(content);
    }

    createDomain();
}

main();