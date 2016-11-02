#!/bin/sh

logfile=$1

nowdate="$(date '+%Y-%m-%d %H:%M:%S')"
deletehour="$(date '+%Y%m%d00' --date="1 month ago")"
deleteday="$(date '+%Y%m%d' --date="6 month ago")"
deletemonth="$(date '+%Y%m' --date="5 year ago")"

nowpath="$(dirname $0)"
db_scr="$nowpath/db_connect.sh"
scrname="$(basename $0)"

# hourly statstics delete

sh $STAT_LOGGER "INFO" "$scrname" "Delete Old Hourly Statics execute" $logfile
sh $db_scr "DELETE FROM est_csr_route_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_csr_route_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_app_route_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_app_route_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_so_route_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_so_route_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_css_route_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_css_route_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_csr_status_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_csr_status_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_app_status_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_app_status_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_so_status_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_so_status_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_css_status_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_css_status_hour WHERE stat_hour < '$deletehour';"
#sh $db_scr "DELETE FROM est_app_session_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
#echo "DELETE FROM est_app_session_hour WHERE stat_hour < '$deletehour';"
#sh $db_scr "DELETE FROM est_so_session_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
#echo "DELETE FROM est_so_session_hour WHERE stat_hour < '$deletehour';"
#sh $db_scr "DELETE FROM est_css_session_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
#echo "DELETE FROM est_css_session_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_so_csrtps_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_so_csrtps_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_csr_tps_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_csr_tps_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_app_uvpv_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_app_uvpv_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_so_uvpv_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_so_uvpv_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_css_uvpv_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_css_uvpv_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_app_ccu_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_app_ccu_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_so_ccu_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_so_ccu_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_css_ccu_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_css_ccu_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_app_usetime_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_app_usetime_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_so_usetime_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_so_usetime_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_css_usetime_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_css_usetime_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_app_bandwidth_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_app_bandwidth_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_so_bandwidth_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_so_bandwidth_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_css_bandwidth_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_css_bandwidth_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_app_sed_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_app_sed_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_so_sed_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_so_sed_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_css_sed_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_css_sed_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_app_web_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_app_web_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_so_web_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_so_web_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_css_web_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_css_web_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_so_webtps_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_so_webtps_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_web_tps_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_web_tps_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_cpu_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_cpu_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_mem_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_mem_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_disk_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_disk_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_network_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_network_hour WHERE stat_hour < '$deletehour';"
sh $db_scr "DELETE FROM est_uplink_hour WHERE stat_hour < '$deletehour';" >> $logfile 2>&1
echo "DELETE FROM est_uplink_hour WHERE stat_hour < '$deletehour';"


# daily statstics delete

echo ""
sh $STAT_LOGGER "INFO" "$scrname" "Delete old Daily Statics execute" $logfile
sh $db_scr "DELETE FROM est_csr_route_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_csr_route_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_app_route_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_app_route_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_so_route_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_so_route_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_css_route_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_css_route_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_csr_status_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_csr_status_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_app_status_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_app_status_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_so_status_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_so_status_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_css_status_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_css_status_day WHERE stat_day < '$deleteday';"
#sh $db_scr "DELETE FROM est_app_session_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
#echo "DELETE FROM est_app_session_day WHERE stat_day < '$deleteday';"
#sh $db_scr "DELETE FROM est_so_session_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
#echo "DELETE FROM est_so_session_day WHERE stat_day < '$deleteday';"
#sh $db_scr "DELETE FROM est_css_session_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
#echo "DELETE FROM est_css_session_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_so_csrtps_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_so_csrtps_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_csr_tps_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_csr_tps_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_app_uvpv_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_app_uvpv_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_so_uvpv_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_so_uvpv_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_css_uvpv_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_css_uvpv_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_app_ccu_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_app_ccu_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_so_ccu_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_so_ccu_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_css_ccu_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_css_ccu_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_app_usetime_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_app_usetime_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_so_usetime_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_so_usetime_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_css_usetime_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_css_usetime_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_app_bandwidth_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_app_bandwidth_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_so_bandwidth_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_so_bandwidth_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_css_bandwidth_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_css_bandwidth_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_app_sed_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_app_sed_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_so_sed_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_so_sed_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_css_sed_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_css_sed_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_app_web_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_app_web_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_so_web_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_so_web_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_css_web_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_css_web_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_so_webtps_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_so_webtps_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_web_tps_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_web_tps_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_cpu_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_cpu_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_mem_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_mem_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_disk_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_disk_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_network_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_network_day WHERE stat_day < '$deleteday';"
sh $db_scr "DELETE FROM est_uplink_day WHERE stat_day < '$deleteday';" >> $logfile 2>&1
echo "DELETE FROM est_uplink_day WHERE stat_day < '$deleteday';"


# monthly statstics delete

echo ""	
sh $STAT_LOGGER "INFO" "$scrname" "Delete Old Monthly Statics execute" $logfile
sh $db_scr "DELETE FROM est_csr_route_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_csr_route_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_app_route_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_app_route_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_so_route_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_so_route_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_css_route_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_css_route_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_csr_status_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_csr_status_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_app_status_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_app_status_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_so_status_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_so_status_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_css_status_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_css_status_month WHERE stat_month < '$deletemonth';"
#sh $db_scr "DELETE FROM est_app_session_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
#echo "DELETE FROM est_app_session_month WHERE stat_month < '$deletemonth';"
#sh $db_scr "DELETE FROM est_so_session_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
#echo "DELETE FROM est_so_session_month WHERE stat_month < '$deletemonth';"
#sh $db_scr "DELETE FROM est_css_session_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
#echo "DELETE FROM est_css_session_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_so_csrtps_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_so_csrtps_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_csr_tps_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_csr_tps_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_app_uvpv_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_app_uvpv_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_so_uvpv_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_so_uvpv_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_css_uvpv_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_css_uvpv_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_app_ccu_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_app_ccu_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_so_ccu_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_so_ccu_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_css_ccu_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_css_ccu_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_app_usetime_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_app_usetime_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_so_usetime_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_so_usetime_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_css_usetime_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_css_usetime_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_app_bandwidth_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_app_bandwidth_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_so_bandwidth_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_so_bandwidth_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_css_bandwidth_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_css_bandwidth_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_app_sed_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_app_sed_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_so_sed_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_so_sed_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_css_sed_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_css_sed_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_app_web_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_app_web_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_so_web_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_so_web_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_css_web_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_css_web_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_so_webtps_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_so_webtps_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_web_tps_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_web_tps_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_cpu_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_cpu_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_mem_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_mem_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_disk_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_disk_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_network_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_network_month WHERE stat_month < '$deletemonth';"
sh $db_scr "DELETE FROM est_uplink_month WHERE stat_month < '$deletemonth';" >> $logfile 2>&1
echo "DELETE FROM est_uplink_month WHERE stat_month < '$deletemonth';"

