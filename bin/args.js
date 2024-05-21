//  --------- 通用配置参数 ---------
var DBUSER = "sdbadmin";
var DBPASSWORD = "sdbadmin";
var COORDADDR = "localhost";
var COORDSVC = "11810";
var db = new Sdb( COORDADDR, COORDSVC, DBUSER, DBPASSWORD);
//  --------------------------------

//  --------- check_model.js 工具参数 ---------
//  $SNAPSHOT_CL 和 $SNAPSHOT_CS 快照是否有 nodeselect = "primary" 条件（收集信息和分析时需要保持一致）
var ISPRIMARY = true;
//  起始年份，如果某些历史表没有当前时间分区的表，需要填写该值让工具从更早的时间开始查找，更长的查找时间段对性能有一定影响。默认值为 0 代表从当前时间查找
//  如某个应用最后一个表为 APP_2022, 此参数需要设置为 2022
var STARTYEAR = 2021;
//  扩展分区结束时间，用于限制扩建表生成的数量，格式为 YYYYMMdd
var ENDTIME = "20250501";
//  sdb cs 名，工具运行中会建 CL 并插入数据，请确保没有其他表，默认值 MODEL
var DATACS = "MODEL";
//  分析运行结束后是否删除 DATACS，默认值 true
var DROPDATACS = false;
//  主表名，用于在集群中查找以下规则命名的主表
//  其中 key 代表表类型，value 代表表命名格式
var MAIN_CL_ARRAY = [{"DOC":"$APPNAME.$APPNAME"}];
//  普通表名，用于在集群中查找以下规则命名的普通表，如同一类型下（LOB）有多种时间格式的表，可使用 $DATE
var NORMAL_CL_ARRAY = [{"LOB":"$APPNAME_LOB_$YYYY.$APPNAME_LOB_$YYYY"}];
//  $DATE 关键字搜索列表，子表命名中时间字符串搜索列表
//  仅支持 "$YYYY","$YYYY$MM","$YYYY$MM$dd" 三种时间组合，可根据实际表名在不同关键字之间插入其他字符如: $YYYY_$MM_$dd
var DATE_FORMAT_ARRAY = ["$YYYY","$YYYY$MM","$YYYY$MM$dd"];
//  ----------------------------------------

//  --------- add.js 工具参数 ---------
//  创建 CL 的 CSV 配置文件路径；如果是扩建现有表时，通常为 output/add_cl.csv 工具分析生成文件；如果是新建应用表，通常为 conf/add_cl.csv 模板文件
var ADDCLCONF = "output/add_cl.csv";
//  CL 和 INDEX 通用配置文件路径，默认值为 conf/config.json
var CONFIGJSON = "conf/config.json";
//  当前模型文件路径，扩建现有表时需要此文件，新建应用表不需要此文件，默认值为 output/current_model.json
var CURRENTMODEL = "output/current_model.json";
//  ----------------------------------------

//  --------- create_domain.js 工具参数 ---------
//  创建 DOMAIN 的 CSV 配置文件路径，默认值为 conf/domain.csv
var DOMIANCSV = "conf/domain.csv";
//  ----------------------------------------

//  -------------- 开发者参数 --------------
// 关键字列表，上面三个 ARRAY 中如果出现 $ 符号，必须是下面这些关键字，否则会报错
// $APPNAME 表示为业务应用名，$DATE 表示匹配 DATE_FORMAT_ARRAY 数组中所有的时间格式, 其余为时间格式标识
var KEYWORDARRAY = ["$APPNAME","$DATE","$YYYY","$MM","$dd"];
//  ----------------------------------------