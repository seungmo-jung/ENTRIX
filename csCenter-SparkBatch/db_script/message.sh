#!/bin/sh

#Usage function
function useage() {
	echo "USEAGE : $(basename $0) [Log type] [Process/Script name] [Log Message] [logfile]"
	exit 1;
}

# if help option print useage
if [ $(echo $@ | grep -e "-h" | grep -v grep | wc -l) -gt 0 ];
then
	useage
fi

# if not correct parameter count, print useage
if [ "$#" != "4" ];
then
	useage
fi

type=$1
pname=$2
msg=$3
logfile=$4

# file log enable check
if [ "$STAT_FILE_LOG_FLAG" == "Y" ];
then
	if [ "$logfile" != "" ];
	then
		echo "[$(date '+%Y%m%d %H:%M:%S.%NS')][$type][$pname] $msg" >> $logfile
		tail -1 $logfile
	else
		echo "[$(date '+%Y%m%d %H:%M:%S.%NS')][$type][$pname] $msg"
	fi
else
	echo "[$(date '+%Y%m%d %H:%M:%S.%NS')][$type][$pname] $msg"
fi

