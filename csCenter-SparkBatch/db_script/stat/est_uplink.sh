#!/bin/sh

# get monthly, yearly statstics sql
function getSQL() {
	local srcname=$1
	local targetname=$2
	local statdate=$3

	local SQL="INSERT INTO est_uplink_$targetname(stat_$targetname, so_id, network_id, avg_rx_bps, min_rx_bps, max_rx_bps, avg_tx_bps, min_tx_bps, max_tx_bps, chg_date) SELECT '$statdate' AS stat_$targetname, so_id, network_id, AVG(avg_rx_bps), MIN(min_rx_bps), MAX(max_rx_bps), AVG(avg_tx_bps), MIN(min_tx_bps), MAX(max_tx_bps), '$nowdate' FROM est_uplink_$srcname WHERE stat_$srcname LIKE '$statdate%' GROUP BY so_id, network_id;"

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
daySQL="INSERT INTO est_uplink_day(stat_day, so_id, network_id, avg_rx_bps, min_rx_bps, max_rx_bps, avg_tx_bps, min_tx_bps, max_tx_bps, chg_date) SELECT '$statday', so_id, network_id, AVG(avg_rx_bps), MIN(min_rx_bps), MAX(max_rx_bps), AVG(avg_tx_bps), MIN(min_tx_bps), MAX(max_tx_bps), '$nowdate' FROM est_uplink_hour WHERE stat_hour LIKE '$statday%' GROUP BY so_id, network_id;"

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

