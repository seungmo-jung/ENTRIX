#!/bin/sh

# get monthly, yearly statstics sql
function getSQL() {
	local srcname=$1
	local targetname=$2
	local statdate=$3

	local SQL="INSERT INTO est_web_tps_$targetname(stat_$targetname, so_id, web_id, avg_tps, min_tps, max_tps, chg_date) SELECT '$statdate' AS stat_$targetname, so_id, web_id, AVG(avg_tps), MIN(min_tps), MAX(max_tps), '$nowdate' FROM est_web_tps_$srcname WHERE stat_$srcname LIKE '$statdate%' GROUP BY so_id, web_id;"

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
daySQL="INSERT INTO est_web_tps_day(stat_day, so_id, web_id, avg_tps, min_tps, max_tps, chg_date) SELECT '$statday', so_id, web_id, AVG(avg_tps), MIN(min_tps), MAX(max_tps), '$nowdate' FROM est_web_tps_hour WHERE stat_hour LIKE '$statday%' GROUP BY so_id, web_id;"

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

