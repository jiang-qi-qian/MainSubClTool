var DBUSER = "";
var DBPASSWORD = "";
var COORDADDR = "localhost";
var COORDSVC = "11810";
var db = new Sdb( COORDADDR, COORDSVC, DBUSER, DBPASSWORD);

// 由分区键范围判断
var ENDTIME = "20320101";

// 落表计算
var dbtmpcs = "";

// 关键字: $APPNAME,$DATE
var MAIN_CL_ARRAY = [{"DOC":"$APPNAME.$APPNAME_DOC"},{"FILE":"$APPNAME.$APPNAME_FILE"}, {"TEST":"$APPNAME.$APPNAME_LOB"}];
//var MAIN_CL_ARRAY = [{"DOC":"$APPNAME.$APPNAME_DOC"},{"FILE":"$APPNAME.$APPNAME_FILE"}];
var NORMAL_CL_ARRAY = [{"LOB":"$APPNAME_LOB_$DATE.$APPNAME_LOB_$DATE"}];
// 2024_01_01 -> YYYY_MM_dd
// 仅支持年月日，且 $YYYY，$MM，$dd 为关键字，表名中不可以有
var DATE_FORMAT_ARRAY = ["$YYYY$MM$dd", "$YYYY$MM", "$YYYY"];
