#!/bin/sh

logfile=$1
debugday=$2
debugmonth=$3
refday=$4

nowdate="$(date '+%Y-%m-%d %H:%M:%S')"
todayday="$(date '+%d')"

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

# daily statstics delete

if [ "$debugday" == "on" ]
then

	sh $STAT_LOGGER "INFO" "$scrname" "Delete Daily Statics execute" $logfile
	sh $db_scr "DELETE FROM est_csr_route_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_csr_route_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_app_route_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_app_route_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_so_route_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_so_route_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_css_route_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_css_route_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_csr_status_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_csr_status_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_app_status_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_app_status_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_so_status_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_so_status_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_css_status_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_css_status_day WHERE stat_day = '$statday';"
#	sh $db_scr "DELETE FROM est_app_session_day WHERE stat_day = '$statday';" >> $logfile 2>&1
#	echo "DELETE FROM est_app_session_day WHERE stat_day = '$statday';"
#	sh $db_scr "DELETE FROM est_so_session_day WHERE stat_day = '$statday';" >> $logfile 2>&1
#	echo "DELETE FROM est_so_session_day WHERE stat_day = '$statday';"
#	sh $db_scr "DELETE FROM est_css_session_day WHERE stat_day = '$statday';" >> $logfile 2>&1
#	echo "DELETE FROM est_css_session_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_so_csrtps_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_so_csrtps_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_csr_tps_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_csr_tps_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_app_uvpv_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_app_uvpv_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_so_uvpv_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_so_uvpv_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_css_uvpv_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_css_uvpv_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_app_ccu_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_app_ccu_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_so_ccu_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_so_ccu_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_css_ccu_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_css_ccu_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_app_usetime_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_app_usetime_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_so_usetime_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_so_usetime_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_css_usetime_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_css_usetime_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_app_bandwidth_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_app_bandwidth_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_so_bandwidth_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_so_bandwidth_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_css_bandwidth_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_css_bandwidth_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_app_sed_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_app_sed_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_so_sed_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_so_sed_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_css_sed_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_css_sed_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_app_web_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_app_web_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_so_web_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_so_web_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_css_web_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_css_web_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_so_webtps_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_so_webtps_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_web_tps_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_web_tps_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_cpu_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_cpu_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_mem_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_mem_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_disk_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_disk_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_network_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_network_day WHERE stat_day = '$statday';"
	sh $db_scr "DELETE FROM est_uplink_day WHERE stat_day = '$statday';" >> $logfile 2>&1
	echo "DELETE FROM est_uplink_day WHERE stat_day = '$statday';"
fi

# date check. if today is 1st day, insert monthly of last month.
if [ "$debugmonth" == "on" ];
then
	echo ""	
	sh $STAT_LOGGER "INFO" "$scrname" "Delete Monthly Statics execute" $logfile
	sh $db_scr "DELETE FROM est_csr_route_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_csr_route_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_app_route_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_app_route_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_so_route_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_so_route_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_css_route_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_css_route_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_csr_status_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_csr_status_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_app_status_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_app_status_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_so_status_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_so_status_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_css_status_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_css_status_month WHERE stat_month = '$statmonth';"
#	sh $db_scr "DELETE FROM est_app_session_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
#	echo "DELETE FROM est_app_session_month WHERE stat_month = '$statmonth';"
#	sh $db_scr "DELETE FROM est_so_session_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
#	echo "DELETE FROM est_so_session_month WHERE stat_month = '$statmonth';"
#	sh $db_scr "DELETE FROM est_css_session_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
#	echo "DELETE FROM est_css_session_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_so_csrtps_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_so_csrtps_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_csr_tps_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_csr_tps_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_app_uvpv_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_app_uvpv_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_so_uvpv_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_so_uvpv_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_css_uvpv_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_css_uvpv_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_app_ccu_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_app_ccu_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_so_ccu_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_so_ccu_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_css_ccu_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_css_ccu_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_app_usetime_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_app_usetime_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_so_usetime_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_so_usetime_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_css_usetime_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_css_usetime_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_app_bandwidth_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_app_bandwidth_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_so_bandwidth_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_so_bandwidth_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_css_bandwidth_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_css_bandwidth_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_app_sed_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_app_sed_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_so_sed_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_so_sed_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_css_sed_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_css_sed_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_app_web_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_app_web_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_so_web_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_so_web_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_css_web_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_css_web_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_so_webtps_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_so_webtps_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_web_tps_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_web_tps_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_cpu_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_cpu_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_mem_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_mem_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_disk_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_disk_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_network_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_network_month WHERE stat_month = '$statmonth';"
	sh $db_scr "DELETE FROM est_uplink_month WHERE stat_month = '$statmonth';" >> $logfile 2>&1
	echo "DELETE FROM est_uplink_month WHERE stat_month = '$statmonth';"
fi

