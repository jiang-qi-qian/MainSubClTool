// --------- 通用配置参数 ---------
var DBUSER = "";
var DBPASSWORD = "";
var COORDADDR = "localhost";
var COORDSVC = "11810";
var db = new Sdb( COORDADDR, COORDSVC, DBUSER, DBPASSWORD);
// --------------------------------

// --------- check_model.js 参数 ---------
// 扩展分区结束时间
var ENDTIME = "20340101";
// sdb cs 名，工具运行中会建 CL 并插入数据，请确保没有其他表
var DATACS = "MODEL";
// 运行结束后删除 DATACS
var DROPDATACS = false;
// 主表名，用于在集群中查找以下规则命名的主表
var MAIN_CL_ARRAY = [{"DOC":"$APPNAME.$APPNAME_DOC"},{"FILE":"$APPNAME.$APPNAME_FILE"}, {"TEST":"$APPNAME.$APPNAME_LOB"}];
// 普通表名，用于在集群中查找以下规则命名的普通表
var NORMAL_CL_ARRAY = [{"LOB":"$APPNAME_LOB_$DATE.$APPNAME_LOB_$DATE"}];
// 仅支持年月日
var DATE_FORMAT_ARRAY = ["$YYYY", "$YYYY$MM$dd", "$YYYY$MM"];
// 关键字列表，上面三个 ARRAY 中如果出现 $ 符号，必须是下面这些关键字，否则会报错
// $APPNAME 表示为业务应用名，$DATE 表示匹配 DATE_FORMAT_ARRAY 的时间, 其余为时间格式标识
var KEYWORDARRAY = ["$APPNAME","$DATE","$YYYY","$MM","$dd","$HH","$mm","$ss"];
// ----------------------------------------

// ------------ Test --------------
//var MAIN_CL_ARRAY = [{"DOC":"$APPNAME.$APPNAME_DOC"}];
//var MAIN_CL_ARRAY = [{"DOC":"$APPNAME_DOC.$APPNAME_DOC"}, {"TEST":"$APPNAME_LOB.$APPNAME_LOB"}];
//var MAIN_CL_ARRAY = [];
//var NORMAL_CL_ARRAY = [];
//var NORMAL_CL_ARRAY = [{"LOB":"$APPNAME_LOB_$DATE.$APPNAME_LOB_$DATE"},{"archive":"bm_test.bm_archive_$APPNAME"},{"meta":"bm_test.bm_$APPNAME_meta"},{"common":"bm_test.bm_common_$APPNAME"},{"file_index":"bm_test.bm_file_index_$APPNAME"},{"file":"bm_test.bm_file_$APPNAME"},{"file_index_test":"bm_test.bm_file_index_test_$APPNAME"}];
//
