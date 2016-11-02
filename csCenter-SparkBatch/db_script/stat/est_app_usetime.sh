#!/bin/sh

# get monthly, yearly statstics sql
function getSQL() {
	local srcname=$1
	local targetname=$2
	local statdate=$3

	local SQL="INSERT INTO est_app_usetime_$targetname(stat_$targetname, app_id, 
c_1_10s, s_1_10s, c_11_20s, s_11_20s, c_21_30s, s_21_30s, c_31_40s, s_31_40s, c_41_50s, s_41_50s, c_51_60s, s_51_60s, 
c_1_2m, s_1_2m, c_2_3m, s_2_3m, c_3_4m, s_3_4m, c_4_5m, s_4_5m, c_5_6m, s_5_6m, c_6_7m, s_6_7m, c_7_8m, s_7_8m, c_8_9m, s_8_9m, c_9_10m, s_9_10m, 
c_10_30m, s_10_30m, c_30_60m, s_30_60m, c_other, s_other, total_cnt, chg_date) SELECT '$statdate' AS stat_$targetname, app_id, 
SUM(c_1_10s), IFNULL((SUM(c_1_10s) / SUM(total_cnt) * 100), '0'), 
SUM(c_11_20s), IFNULL((SUM(c_11_20s) / SUM(total_cnt) * 100), '0'), 
SUM(c_21_30s), IFNULL((SUM(c_21_30s) / SUM(total_cnt) * 100), '0'), 
SUM(c_31_40s), IFNULL((SUM(c_31_40s) / SUM(total_cnt) * 100), '0'), 
SUM(c_41_50s), IFNULL((SUM(c_41_50s) / SUM(total_cnt) * 100), '0'), 
SUM(c_51_60s), IFNULL((SUM(c_51_60s) / SUM(total_cnt) * 100), '0'), 
SUM(c_1_2m), IFNULL((SUM(c_1_2m) / SUM(total_cnt) * 100), '0'), 
SUM(c_2_3m), IFNULL((SUM(c_2_3m) / SUM(total_cnt) * 100), '0'), 
SUM(c_3_4m), IFNULL((SUM(c_3_4m) / SUM(total_cnt) * 100), '0'), 
SUM(c_4_5m), IFNULL((SUM(c_4_5m) / SUM(total_cnt) * 100), '0'), 
SUM(c_5_6m), IFNULL((SUM(c_5_6m) / SUM(total_cnt) * 100), '0'), 
SUM(c_6_7m), IFNULL((SUM(c_6_7m) / SUM(total_cnt) * 100), '0'), 
SUM(c_7_8m), IFNULL((SUM(c_7_8m) / SUM(total_cnt) * 100), '0'), 
SUM(c_8_9m), IFNULL((SUM(c_8_9m) / SUM(total_cnt) * 100), '0'), 
SUM(c_9_10m), IFNULL((SUM(c_9_10m) / SUM(total_cnt) * 100), '0'), 
SUM(c_10_30m), IFNULL((SUM(c_10_30m) / SUM(total_cnt) * 100), '0'), 
SUM(c_30_60m), IFNULL((SUM(c_30_60m) / SUM(total_cnt) * 100), '0'), 
SUM(c_other), IFNULL((SUM(c_other) / SUM(total_cnt) * 100), '0'), 
SUM(total_cnt), '$nowdate' FROM est_app_usetime_$srcname WHERE stat_$srcname LIKE '$statdate%' GROUP BY app_id;"

	echo "$SQL"
}

logfile=$1
debugday=$2
debugmonth=$3
refday=$4

nowdate="$(date '+%Y-%m-%d %H:%M:%S')"
todayday="$(date '+%d')"
todaymonth="$(date '+%m%d')"

nowpath="$(dirname $0)"
db_scr="$nowpath/db_connect.sh"
scrname="$(basename $0)"

if [ "$debugday" == "on" ]
then
        statday="$(date '+%Y%m%d' --date="$refday days ago")"
else
        statday="$(date '+%Y%m%d' --date="1 days ago")"
fi

if [ "$debugmonth" == "on" ]
then
        statmonth="$(date '+%Y%m' --date="$refday days ago")"
else
        statmonth="$(date '+%Y%m' --date="1 month ago")"
fi

# daily statstics insert
daySQL="INSERT INTO est_app_usetime_day(stat_day, app_id, 
c_1_10s, s_1_10s, c_11_20s, s_11_20s, c_21_30s, s_21_30s, c_31_40s, s_31_40s, c_41_50s, s_41_50s, c_51_60s, s_51_60s, 
c_1_2m, s_1_2m, c_2_3m, s_2_3m, c_3_4m, s_3_4m, c_4_5m, s_4_5m, c_5_6m, s_5_6m, c_6_7m, s_6_7m, c_7_8m, s_7_8m, c_8_9m, s_8_9m, c_9_10m, s_9_10m, 
c_10_30m, s_10_30m, c_30_60m, s_30_60m, c_other, s_other, total_cnt, chg_date) SELECT '$statday', app_id, 
SUM(c_1_10s), IFNULL((SUM(c_1_10s) / SUM(total_cnt) * 100), '0'), 
SUM(c_11_20s), IFNULL((SUM(c_11_20s) / SUM(total_cnt) * 100), '0'), 
SUM(c_21_30s), IFNULL((SUM(c_21_30s) / SUM(total_cnt) * 100), '0'), 
SUM(c_31_40s), IFNULL((SUM(c_31_40s) / SUM(total_cnt) * 100), '0'), 
SUM(c_41_50s), IFNULL((SUM(c_41_50s) / SUM(total_cnt) * 100), '0'), 
SUM(c_51_60s), IFNULL((SUM(c_51_60s) / SUM(total_cnt) * 100), '0'), 
SUM(c_1_2m), IFNULL((SUM(c_1_2m) / SUM(total_cnt) * 100), '0'), 
SUM(c_2_3m), IFNULL((SUM(c_2_3m) / SUM(total_cnt) * 100), '0'), 
SUM(c_3_4m), IFNULL((SUM(c_3_4m) / SUM(total_cnt) * 100), '0'), 
SUM(c_4_5m), IFNULL((SUM(c_4_5m) / SUM(total_cnt) * 100), '0'), 
SUM(c_5_6m), IFNULL((SUM(c_5_6m) / SUM(total_cnt) * 100), '0'), 
SUM(c_6_7m), IFNULL((SUM(c_6_7m) / SUM(total_cnt) * 100), '0'), 
SUM(c_7_8m), IFNULL((SUM(c_7_8m) / SUM(total_cnt) * 100), '0'), 
SUM(c_8_9m), IFNULL((SUM(c_8_9m) / SUM(total_cnt) * 100), '0'), 
SUM(c_9_10m), IFNULL((SUM(c_9_10m) / SUM(total_cnt) * 100), '0'), 
SUM(c_10_30m), IFNULL((SUM(c_10_30m) / SUM(total_cnt) * 100), '0'), 
SUM(c_30_60m), IFNULL((SUM(c_30_60m) / SUM(total_cnt) * 100), '0'), 
SUM(c_other), IFNULL((SUM(c_other) / SUM(total_cnt) * 100), '0'), 
SUM(total_cnt), '$nowdate' FROM est_app_usetime_hour WHERE stat_hour LIKE '$statday%' GROUP BY app_id;"

sh $STAT_LOGGER "INFO" "$scrname" "Daily Service statics execute" $logfile
sh $db_scr "$daySQL" >> $logfile 2>&1
echo "$daySQL"

# date check. if today is 1st day, insert monthly of last month.
if [ "$todayday" == "01" ] || [ "$debugmonth" == "on" ];
then
	monSQL="$(getSQL day month $statmonth)"
	
	sh $STAT_LOGGER "INFO" "$scrname" "Monthly Service statics execute" $logfile
	sh $db_scr "$monSQL" >> $logfile 2>&1
	echo "$monSQL"
fi

