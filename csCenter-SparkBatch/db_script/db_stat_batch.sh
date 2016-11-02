#!/bin/sh

nowpath="$(dirname $0)"
scrname="$(basename $0)"
conffile="$nowpath/${scrname%.*}.cfg"

today="$(date '+%Y%m%d')"
export STAT_LOGGER="$nowpath/message.sh"

# Config file check
if [ ! -e $conffile ];
then
        sh $STAT_LOGGER "ERROR" "$scrname" "Need config file($conffile). Can't execute" "$logfile"
        exit 1;
fi

# Setting up config value by config file
while read confline
do
        if [ "${confline:0:1}" != "#" ];
        then
                confname="$(echo $confline | awk -F= '{print $1}')"
                confval="$(echo $confline | awk -F= '{print $2}')"
                case $confname in
                        LOGPATH)
                                logpath="$confval"
                        ;;
                        FILELOG)
                                filelogflag="$confval"
                        ;;
                        DB_IP)
                                dbip="$confval"
                        ;;
                        DB_PORT)
                                dbport="$confval"
                        ;;
                        DB_USER)
                                dbuser="$confval"
                        ;;
                        DB_PASS)
                                dbpass="$confval"
                        ;;
                        DB_NAME)
                                dbname="$confval"
                        ;;
                        DEBUG_DAY)
                                debugday="$confval"
                        ;;
                        DEBUG_MONTH)
                                debugmonth="$confval"
                        ;;
                        STAT_DAY)
                                statday="$confval"
                        ;;
                        UVPV_DAY)
                                uvpvday="$confval"
                        ;;
                esac
        fi
done < $conffile

# Script log path check. if not set up default log path location.
export STAT_LOGPATH="$logpath"
logfile="$STAT_LOGPATH/stat_batch_log_$today.log"

if [ ! -e $logpath ];
then
        logpath="$nowpath/log"
        export STAT_LOGPATH="$logpath"
        logfile="$STAT_LOGPATH/stat_batch_log_$today.log"
        sh $STAT_LOGGER "WARN" "$scrname" "logpath is not exist or defined. set locally path($logpath)" $logfile
fi

# check db connection configuration. if not set up default configuration
# CAUTION : do not check password and schema name

# ip check
if [ "$dbip" == "" ];
then
        dbip="localhost"
fi

# port check
if [ "$dbport" == "" ];
then
        dbport="3306"
fi

# user id check
if [ "$dbuser" == "" ];
then
        dbuser="root"
fi

if [ "$debugday" == "" ];
then
        debugday="off"
fi

if [ "$debugmonth" == "" ];
then
        debugmonth="off"
fi

if [ "$statday" == "" ];
then
        statday="0"
fi

export STAT_FILE_LOG_FLAG="$filelogflag"
export STAT_DB_IP="$dbip"
export STAT_DB_PORT="$dbport"
export STAT_DB_USER="$dbuser"
export STAT_DB_PASS="$dbpass"
export STAT_DB_NAME="$dbname"

sh $STAT_LOGGER "INFO" "$scrname" "Start DB Statistic Batch Script" $logfile


################################################################################
#
# Stat script define
#

scr_path="$nowpath/stat"

# for delete data
est_delete_old_src="$scr_path/est_delete_old.sh"
est_delete_debugday_src="$scr_path/est_delete_debugday.sh"

# for CSR log
est_csr_route_src="$scr_path/est_csr_route.sh"
est_app_route_src="$scr_path/est_app_route.sh"
est_so_route_src="$scr_path/est_so_route.sh"
est_css_route_src="$scr_path/est_css_route.sh"
est_csr_status_src="$scr_path/est_csr_status.sh"
est_app_status_src="$scr_path/est_app_status.sh"
est_so_status_src="$scr_path/est_so_status.sh"
est_css_status_src="$scr_path/est_css_status.sh"
#est_app_session_src="$scr_path/est_app_session.sh"
#est_so_session_src="$scr_path/est_so_session.sh"
#est_css_session_src="$scr_path/est_css_session.sh"
est_so_csrtps_src="$scr_path/est_so_csrtps.sh"
est_csr_tps_src="$scr_path/est_csr_tps.sh"

# for STAT log
est_app_uvpv_src="$scr_path/est_app_uvpv.sh"
est_so_uvpv_src="$scr_path/est_so_uvpv.sh"
est_css_uvpv_src="$scr_path/est_css_uvpv.sh"
est_app_ccu_src="$scr_path/est_app_ccu.sh"
est_so_ccu_src="$scr_path/est_so_ccu.sh"
est_css_ccu_src="$scr_path/est_css_ccu.sh"
est_app_usetime_src="$scr_path/est_app_usetime.sh"
est_so_usetime_src="$scr_path/est_so_usetime.sh"
est_css_usetime_src="$scr_path/est_css_usetime.sh"
est_app_bandwidth_src="$scr_path/est_app_bandwidth.sh"
est_so_bandwidth_src="$scr_path/est_so_bandwidth.sh"
est_css_bandwidth_src="$scr_path/est_css_bandwidth.sh"

# for SED log
est_app_sed_src="$scr_path/est_app_sed.sh"
est_so_sed_src="$scr_path/est_so_sed.sh"
est_css_sed_src="$scr_path/est_css_sed.sh"

# for WEB log
est_app_web_src="$scr_path/est_app_web.sh"
est_so_web_src="$scr_path/est_so_web.sh"
est_css_web_src="$scr_path/est_css_web.sh"
est_so_webtps_src="$scr_path/est_so_webtps.sh"
est_web_tps_src="$scr_path/est_web_tps.sh"

# for Matric log
est_cpu_src="$scr_path/est_cpu.sh"
est_mem_src="$scr_path/est_mem.sh"
est_disk_src="$scr_path/est_disk.sh"
est_network_src="$scr_path/est_network.sh"
est_uplink_src="$scr_path/est_uplink.sh"


################################################################################
#
# for delete data 
#

# Execute DELETE old stat script file
if [ -e $est_delete_old_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute DELETE old stat script" $logfile
        sh $est_delete_old_src $logfile 
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find DELETE old stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute DELETE debugday script file
if [ -e $est_delete_debugday_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute DELETE stat script" $logfile
        sh $est_delete_debugday_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find DELETE stat script file in ($scr_path)" $logfile
fi
echo ""


################################################################################
#
# for CSR log
#

# Execute CSR ROUTE stat script file
if [ -e $est_csr_route_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute CSR ROUTE stat script" $logfile
        sh $est_csr_route_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find CSR ROUTE stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute APP ROUTE stat script file
if [ -e $est_app_route_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute APP ROUTE stat script" $logfile
        sh $est_app_route_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find APP ROUTE stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute SO ROUTE stat script file
if [ -e $est_so_route_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute SO ROUTE stat script" $logfile
        sh $est_so_route_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find SO ROUTE stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute CSS ROUTE stat script file
if [ -e $est_css_route_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute CSS ROUTE stat script" $logfile
        sh $est_css_route_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find CSS ROUTE stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute CSR STATUS stat script file
if [ -e $est_csr_status_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute CSR STATUS stat script" $logfile
        sh $est_csr_status_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find CSR STATUS stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute APP STATUS stat script file
if [ -e $est_app_status_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute APP STATUS stat script" $logfile
        sh $est_app_status_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find APP STATUS stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute SO STATUS stat script file
if [ -e $est_so_status_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute SO STATUS stat script" $logfile
        sh $est_so_status_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find SO STATUS stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute CSS STATUS stat script file
if [ -e $est_css_status_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute CSS STATUS stat script" $logfile
        sh $est_css_status_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find CSS STATUS stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute APP SESSION stat script file
#if [ -e $est_app_session_src ];
#then
#        sh $STAT_LOGGER "INFO" "$scrname" "Execute APP SESSION stat script" $logfile
#        sh $est_app_session_src $logfile $debugday $debugmonth $statday
#else
#        sh $STAT_LOGGER "WARN" "$scrname" "Can't find APP SESSION stat script file in ($scr_path)" $logfile
#fi
#echo ""

# Execute SO SESSION stat script file
#if [ -e $est_so_session_src ];
#then
#        sh $STAT_LOGGER "INFO" "$scrname" "Execute SO SESSION stat script" $logfile
#        sh $est_so_session_src $logfile $debugday $debugmonth $statday
#else
#        sh $STAT_LOGGER "WARN" "$scrname" "Can't find SO SESSION stat script file in ($scr_path)" $logfile
#fi
#echo ""

# Execute CSS SESSION stat script file
#if [ -e $est_css_session_src ];
#then
#        sh $STAT_LOGGER "INFO" "$scrname" "Execute CSS SESSION stat script" $logfile
#        sh $est_css_session_src $logfile $debugday $debugmonth $statday
#else
#        sh $STAT_LOGGER "WARN" "$scrname" "Can't find CSS SESSION stat script file in ($scr_path)" $logfile
#fi
#echo ""

# Execute SO CSRTPS stat script file
if [ -e $est_so_csrtps_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute SO CSRTPS stat script" $logfile
        sh $est_so_csrtps_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find SO CSRTPS stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute CSR TPS stat script file
if [ -e $est_csr_tps_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute CSR TPS stat script" $logfile
        sh $est_csr_tps_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find CSR TPS stat script file in ($scr_path)" $logfile
fi
echo ""


################################################################################
#
# for STAT log
#

# Execute APP UVPV stat script file
if [ -e $est_app_uvpv_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute APP UVPV stat script" $logfile
        sh $est_app_uvpv_src $logfile $debugday $debugmonth $statday $uvpvday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find APP UVPV stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute SO UVPV stat script file
if [ -e $est_so_uvpv_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute SO UVPV stat script" $logfile
        sh $est_so_uvpv_src $logfile $debugday $debugmonth $statday $uvpvday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find SO UVPV stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute CSS UVPV stat script file
if [ -e $est_css_uvpv_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute CSS UVPV stat script" $logfile
        sh $est_css_uvpv_src $logfile $debugday $debugmonth $statday $uvpvday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find CSS UVPV stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute APP CCU stat script file
if [ -e $est_app_ccu_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute APP CCU stat script" $logfile
        sh $est_app_ccu_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find APP CCU stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute SO CCU stat script file
if [ -e $est_so_ccu_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute SO CCU stat script" $logfile
        sh $est_so_ccu_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find SO CCU stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute CSS CCU stat script file
if [ -e $est_css_ccu_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute CSS CCU stat script" $logfile
        sh $est_css_ccu_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find CSS CCU stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute APP USETIME stat script file
if [ -e $est_app_usetime_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute APP USETIME stat script" $logfile
        sh $est_app_usetime_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find APP USETIME stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute SO USETIME stat script file
if [ -e $est_so_usetime_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute SO USETIME stat script" $logfile
        sh $est_so_usetime_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find SO USETIME stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute CSS USETIME stat script file
if [ -e $est_css_usetime_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute CSS USETIME stat script" $logfile
        sh $est_css_usetime_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find CSS USETIME stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute APP BANDWIDTH stat script file
if [ -e $est_app_bandwidth_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute APP BANDWIDTH stat script" $logfile
        sh $est_app_bandwidth_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find APP BANDWIDTH stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute SO BANDWIDTH stat script file
if [ -e $est_so_bandwidth_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute SO BANDWIDTH stat script" $logfile
        sh $est_so_bandwidth_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find SO BANDWIDTH stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute CSS BANDWIDTH stat script file
if [ -e $est_css_bandwidth_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute CSS BANDWIDTH stat script" $logfile
        sh $est_css_bandwidth_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find CSS BANDWIDTH stat script file in ($scr_path)" $logfile
fi
echo ""


################################################################################
#
# for SED log
#

# Execute APP SED stat script file
if [ -e $est_app_sed_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute APP SED stat script" $logfile
        sh $est_app_sed_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find APP SED stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute SO SED stat script file
if [ -e $est_so_sed_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute SO SED stat script" $logfile
        sh $est_so_sed_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find SO SED stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute CSS SED stat script file
if [ -e $est_css_sed_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute CSS SED stat script" $logfile
        sh $est_css_sed_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find CSS SED stat script file in ($scr_path)" $logfile
fi
echo ""


################################################################################
#
# for WEB log
#

# Execute APP WEB stat script file
if [ -e $est_app_web_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute APP WEB stat script" $logfile
        sh $est_app_web_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find APP WEB stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute SO WEB stat script file
if [ -e $est_so_web_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute SO WEB stat script" $logfile
        sh $est_so_web_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find SO WEB stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute CSS WEB stat script file
if [ -e $est_css_web_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute CSS WEB stat script" $logfile
        sh $est_css_web_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find CSS WEB stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute SO WEBTPS stat script file
if [ -e $est_so_webtps_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute SO WEBTPS stat script" $logfile
        sh $est_so_webtps_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find SO WEBTPS stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute WEB TPS stat script file
if [ -e $est_web_tps_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute WEB TPS stat script" $logfile
        sh $est_web_tps_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find WEB TPS stat script file in ($scr_path)" $logfile
fi
echo ""


################################################################################
#
# for Matric log
#

# Execute CPU stat script file
if [ -e $est_cpu_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute CPU stat script" $logfile
        sh $est_cpu_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find CPU stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute MEM stat script file
if [ -e $est_mem_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute MEM stat script" $logfile
        sh $est_mem_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find MEM stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute DISK stat script file
if [ -e $est_disk_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute DISK stat script" $logfile
        sh $est_disk_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find DISK stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute NETWORK stat script file
if [ -e $est_network_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute NETWORK stat script" $logfile
        sh $est_network_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find NETWORK stat script file in ($scr_path)" $logfile
fi
echo ""

# Execute UPLINK stat script file
if [ -e $est_uplink_src ];
then
        sh $STAT_LOGGER "INFO" "$scrname" "Execute UPLINK stat script" $logfile
        sh $est_uplink_src $logfile $debugday $debugmonth $statday
else
        sh $STAT_LOGGER "WARN" "$scrname" "Can't find UPLINK stat script file in ($scr_path)" $logfile
fi
echo ""

