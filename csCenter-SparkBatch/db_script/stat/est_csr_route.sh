#!/bin/sh

# get monthly, yearly statstics sql
function getSQL() {
	local srcname=$1
	local targetname=$2
	local statdate=$3

	local SQL="INSERT INTO est_csr_route_$targetname(stat_$targetname, csr_id, req_cnt, success_cnt, fail_cnt, success_rate, chg_date) SELECT '$statdate' AS stat_$targetname, csr_id, SUM(req_cnt), SUM(success_cnt), SUM(fail_cnt), IFNULL((SUM(success_cnt) / SUM(req_cnt) * 100), '0'), '$nowdate' FROM est_csr_route_$srcname WHERE stat_$srcname LIKE '$statdate%' GROUP BY csr_id;"

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
daySQL="INSERT INTO est_csr_route_day(stat_day, csr_id, req_cnt, success_cnt, fail_cnt, success_rate, chg_date) SELECT '$statday', csr_id, SUM(req_cnt), SUM(success_cnt), SUM(fail_cnt), IFNULL((SUM(success_cnt) / SUM(req_cnt) * 100), '0'), '$nowdate' FROM est_csr_route_hour WHERE stat_hour LIKE '$statday%' GROUP BY csr_id;"

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

