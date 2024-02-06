/*
   通用函数
*/

if (typeof operate == "undefined" || operate == null || operate == "") {
    operate = false;
}

//格式化时间工具类
Date.prototype.Format = function (fmt) { // author: meizz
    var o = {
        "M+": this.getMonth() + 1, // 月份
        "d+": this.getDate(), // 日
        "h+": this.getHours(), // 小时
        "m+": this.getMinutes(), // 分
        "s+": this.getSeconds(), // 秒
        "q+": Math.floor((this.getMonth() + 3) / 3), // 季度
        "S": this.getMilliseconds() // 毫秒
    };
    if (/(y+)/.test(fmt))
        fmt = fmt.replace(RegExp.$1, (this.getFullYear() + "").substr(4 - RegExp.$1.length));
    for (var k in o)
        if (new RegExp("(" + k + ")").test(fmt)) fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (("00" + o[k]).substr(("" + o[k]).length)));
    return fmt;
}

var log_file = 'log/' + TOOL + "_" + MODE + "_" + new Date().Format("yyyy-MM-dd hh:mm:ss") + ".log";
var log;

try {
    log = new File(log_file);
    log.seek(0, 'e');
} catch (e) {
    throw e;
}

/**
 * @discription :  初始化日志对象
 * @author: zhonghuajun
 * @return: void
 */
const logger = {
    // log_level 日志等级
    // content 日志内容
    write: function (log_level, content) {
        info = new Date().Format("yyyy-MM-dd hh:mm:ss") + " - [" + log_level + "]" + " " + content;
        println(info)
        log.write(info + " \n");
    },
    // content 日志内容
    info: function (content) {
        this.write("INFO", content);
    },
    // content 日志内容
    warn: function (content) {
        this.write("WARN", content);
    },
    // e 错误对象
    error: function (e) {
        let type = Object.prototype.toString.call(e);
        // 如果是数值类型错误
        // 可能有两种 分别是sdb错误与linux错误
        if (type == '[object Number]') {
            if (e < 0) {
                this.write("ERROR", getErr(e) + "(" + e + ")");
            } else {
                this.write("UNKNOW ERROR", " " + e + " ");
            }
        }
        // 如果是对象类型错误 则是编码错误
        else if (type == '[object Error]') {
            let message = e.stack.split("\n")[0];
            this.write("ERROR", message);
        }
        // 其它类型错误 则是自定义错误 
        else {
            this.write("ERROR", e);
        }
    },
    except: function (info, e) {
        this.error(info)
        this.error(e)
    },
    // 关闭日志对象
    close: function () {
        if (typeof log != "undefined" && log != null && log != "") {
            log.close();
        }
    }
}

/**
 * 拼接\t
 */
function tab() {
    return Array.prototype.slice.call(arguments).join(",")
}