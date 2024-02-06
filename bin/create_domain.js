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
                logger.warn("Domain [" + domainName + "(" + groupArray + ")] is exists");
                continue;
            } catch (e) {
                if (-214 == e) {
                    try {
                        db.createDomain(domainName, groupArray, { AutoSplit: true });
                        logger.info("Create domain [" + domainName + "(" + groupArray + ")] success");
                    } catch (error) {
                        logger.except("Failed to init the domain [" + domainName + "]", error);
                        throw error;
                    }
                } else {
                    logger.except("Failed to init the domain [" + domainName + "]", e);
                    throw e;
                }
            }
        // 回滚
        } else if (MODE == "rollback") {
            try {
                let cs_num = db.getDomain(domainName).listCollectionSpaces().size();
                if (cs_num != 0){
                    logger.error("The domain [" + domainName + "] cannot be deleted because the number of CS in the domain is not zero");
                    continue;
                }
                try {
                    db.dropDomain(domainName);
                    logger.info("Drop domain [" + domainName + "(" + groupArray + ")] success...");
                } catch (error) {
                    logger.except("Failed to drop the domain [" + domainName + "]", error);
                    throw error;
                }
            } catch (e) {
                if (-214 == e) {
                    logger.warn("Domain [" + domainName + "(" + groupArray + ")] is not exists");
                } else {
                    logger.except("Failed to check if the domain [" + domainName + "] exists", e);
                    throw e;
                }
            }
        // 测试
        } else if (MODE == "test") {
            logger.info("Create " + domainName + " with groups (" + groupArray + ")");
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
    logger.info("MODE is " + MODE);
    createDomain();
}

main();