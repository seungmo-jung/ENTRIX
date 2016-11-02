Streamin_home="/app/yarn/cscenter_spark-streaming"
clspath="$Streamin_home/analysis.batch-0.0.1.jar:$Streamin_home/lib/*"
echo "cp=$clspath"
java -Dlog4j.configuration=configs/log4j.properties -cp $clspath com.entrixco.cscenter.analysis.batch.mysql.JdbcMain
