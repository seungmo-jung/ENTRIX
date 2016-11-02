#!/bin/sh

stattype=$1
startdate=$2
enddate=$3

nowpath="$(dirname $0)"
scrname="$(basename $0)"
conffile="$nowpath/db_stat_batch.cfg"
db_scr="$nowpath/stat/db_connect.sh"

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

export STAT_FILE_LOG_FLAG="$filelogflag"
export STAT_DB_IP="$dbip"
export STAT_DB_PORT="$dbport"
export STAT_DB_USER="$dbuser"
export STAT_DB_PASS="$dbpass"
export STAT_DB_NAME="$dbname"

if [ "$enddate" == "" ]
then 
	enddate=$startdate
fi

if [ "$stattype" == "hour" ] 
then 
	if [ ${#startdate} != 10 ]
	then 
		echo "invalid starthour, use \"db_stat_delete.sh hour yyyyMMddHH [yyyyMMddHH]\""
		exit 1
	fi
	if !([[ $startdate =~ ^[0-9]+$ ]])
	then 
		echo "invalid starthour, use \"db_stat_delete.sh hour yyyyMMddHH [yyyyMMddHH]\""
		exit 1
	fi
	if [ ${#enddate} != 10 ]
	then 
		echo "invalid endhour, use \"db_stat_delete.sh hour yyyyMMddHH [yyyyMMddHH]\""
		exit 1
	fi
	if !([[ $enddate =~ ^[0-9]+$ ]])
	then 
		echo "invalid endhour, use \"db_stat_delete.sh hour yyyyMMddHH [yyyyMMddHH]\""
		exit 1
	fi

elif [ "$stattype" == "day" ]
then
	if [ ${#startdate} != 8 ]
	then 
		echo "invalid startday, use \"db_stat_delete.sh day yyyyMMddHH [yyyyMMddHH]\""
		exit 1
	fi
	if !([[ $startdate =~ ^[0-9]+$ ]])
	then 
		echo "invalid startday, use \"db_stat_delete.sh day yyyyMMddHH [yyyyMMddHH]\""
		exit 1
	fi
	if [ ${#enddate} != 8 ]
	then 
		echo "invalid endday, use \"db_stat_delete.sh day yyyyMMddHH [yyyyMMddHH]\""
		exit 1
	fi
	if !([[ $enddate =~ ^[0-9]+$ ]])
	then 
		echo "invalid endday, use \"db_stat_delete.sh day yyyyMMddHH [yyyyMMddHH]\""
		exit 1
	fi

elif [ "$stattype" == "month" ]
then
	if [ ${#startdate} != 6 ]
	then 
		echo "invalid startmonth, use \"db_stat_delete.sh month yyyyMMddHH [yyyyMMddHH]\""
		exit 1
	fi
	if !([[ $startdate =~ ^[0-9]+$ ]])
	then 
		echo "invalid startmonth, use \"db_stat_delete.sh month yyyyMMddHH [yyyyMMddHH]\""
		exit 1
	fi
	if [ ${#enddate} != 6 ]
	then 
		echo "invalid endmonth, use \"db_stat_delete.sh month yyyyMMddHH [yyyyMMddHH]\""
		exit 1
	fi
	if !([[ $enddate =~ ^[0-9]+$ ]])
	then 
		echo "invalid endmonth, use \"db_stat_delete.sh month yyyyMMddHH [yyyyMMddHH]\""
		exit 1
	fi

else
	echo "invalid input, use \"db_stat_delete.sh hour|day|month startdate [enddate]\""
	exit 1
fi


sh $STAT_LOGGER "INFO" "$scrname" "" $logfile
sh $STAT_LOGGER "INFO" "$scrname" "Delete Statistics for User Request \"db_stat_delete.sh $stattype $startdate $enddate\"" $logfile

sh $db_scr "DELETE FROM est_csr_route_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_csr_route_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_app_route_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_app_route_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_so_route_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_so_route_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_css_route_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_css_route_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_csr_status_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_csr_status_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_app_status_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_app_status_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_so_status_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_so_status_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_css_status_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_css_status_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
#sh $db_scr "DELETE FROM est_app_session_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
#echo "DELETE FROM est_app_session_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
#sh $db_scr "DELETE FROM est_so_session_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
#echo "DELETE FROM est_so_session_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
#sh $db_scr "DELETE FROM est_css_session_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
#echo "DELETE FROM est_css_session_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_so_csrtps_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_so_csrtps_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_csr_tps_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_csr_tps_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_app_uvpv_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_app_uvpv_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_so_uvpv_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_so_uvpv_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_css_uvpv_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_css_uvpv_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_app_ccu_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_app_ccu_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_so_ccu_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_so_ccu_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_css_ccu_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_css_ccu_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_app_usetime_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_app_usetime_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_so_usetime_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_so_usetime_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_css_usetime_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_css_usetime_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_app_bandwidth_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_app_bandwidth_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_so_bandwidth_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_so_bandwidth_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_css_bandwidth_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_css_bandwidth_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_app_sed_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_app_sed_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_so_sed_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_so_sed_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_css_sed_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_css_sed_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_app_web_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_app_web_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_so_web_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_so_web_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_css_web_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_css_web_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_so_webtps_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_so_webtps_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_web_tps_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_web_tps_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_cpu_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_cpu_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_mem_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_mem_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_disk_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_disk_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_network_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_network_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"
sh $db_scr "DELETE FROM est_uplink_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';" >> $logfile 2>&1
echo "DELETE FROM est_uplink_$stattype WHERE stat_$stattype >= '$startdate' and stat_$stattype <= '$enddate';"

