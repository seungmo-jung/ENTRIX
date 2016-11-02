#!/bin/sh

# query execute by mysql command
SQL=$1

scrname="$(basename $0)"

if [ "$SQL" == "" ];
then
	sh $STAT_LOGGER "ERROR" "$scrname" "Need SQL query."
	exit 1;
fi

mysql_cmd="mysql -h$STAT_DB_IP -P$STAT_DB_PORT -u$STAT_DB_USER -p$STAT_DB_PASS $STAT_DB_NAME"
echo "$SQL" | $mysql_cmd

