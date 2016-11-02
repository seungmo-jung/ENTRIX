#!/bin/sh

# get monthly, yearly statstics sql
function getSQL() {
	local srcname=$1
	local targetname=$2
	local statdate=$3

	local SQL="INSERT INTO est_app_ccu_$targetname(stat_$targetname, app_id, avg_ccu, min_ccu, max_ccu, chg_date) SELECT '$statdate' AS stat_$targetname, app_id, AVG(avg_ccu), MIN(min_ccu), MAX(max_ccu), '$nowdate' FROM est_app_ccu_$srcname WHERE stat_$srcname LIKE '$statdate%' GROUP BY app_id;"

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
daySQL="INSERT INTO est_app_ccu_day(stat_day, app_id, avg_ccu, min_ccu, max_ccu, chg_date) SELECT '$statday', app_id, AVG(avg_ccu), MIN(min_ccu), MAX(max_ccu), '$nowdate' FROM est_app_ccu_hour WHERE stat_hour LIKE '$statday%' GROUP BY app_id;"

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

