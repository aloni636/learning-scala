$SPARK_HOME/sbin/start-master.sh  # UI port: 8080
$SPARK_HOME/sbin/start-worker.sh spark://$(hostname):7077  # UI port: 8081
$SPARK_HOME/sbin/start-history-server.sh  # UI port: 18080