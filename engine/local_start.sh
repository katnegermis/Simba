export SPARK_MASTER_IP=127.0.0.1

if [ "$SPARK_MASTER_PORT" = "" ]; then
    export SPARK_MASTER_PORT=7077
fi


# Start master
echo "Starting spark master on $SPARK_MASTER_IP:$SPARK_MASTER_PORT"
sbin/start-master.sh

# Start slave
if [ "$SPARK_SLAVE_CORES" = "" ]; then
  export SPARK_SLAVE_CORES=4
fi

if [ "$SPARK_SLAVE_MEMORY" = "" ]; then
  export SPARK_SLAVE_MEMORY=4g
fi

sbin/start-slave.sh -c $SPARK_SLAVE_CORES -m $SPARK_SLAVE_MEMORY "spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT"
