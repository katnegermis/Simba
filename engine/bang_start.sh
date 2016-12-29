export SPARK_MASTER_IP=127.0.0.1
export SPARK_MASTER_PORT=7077
sbin/start-master.sh


if [ "$SPARK_SLAVE_CORES" = "" ]; then
  export SPARK_SLAVE_CORES=4
fi

if [ "$SPARK_SLAVE_MEMORY" = "" ]; then
  export SPARK_SLAVE_MEMORY=11G
fi

sbin/start-slave.sh -c $SPARK_SLAVE_CORES -m $SPARK_SLAVE_MEMORY "spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT"
