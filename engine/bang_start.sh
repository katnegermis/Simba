export SPARK_MASTER_IP=$(ifconfig | grep enp1s0f1 -A 1 |  grep inet | tr -s ' ' | cut -d' ' -f3)

if [ "$SPARK_MASTER_PORT" = "" ]; then
    export SPARK_MASTER_PORT=7077
fi


# Start master
echo "Starting spark master on $SPARK_MASTER_IP:$SPARK_MASTER_PORT"
sbin/start-master.sh

# Start slave
if [ "$SPARK_SLAVE_CORES" = "" ]; then
  export SPARK_SLAVE_CORES=32
fi

if [ "$SPARK_SLAVE_MEMORY" = "" ]; then
  export SPARK_SLAVE_MEMORY=80g
fi

export SPARK_HOME=/home/tqg432/simba/Simba/engine/

CMD=$(echo "cd . && cd $SPARK_HOME; pwd; sbin/start-slave.sh -c $SPARK_SLAVE_CORES -m $SPARK_SLAVE_MEMORY \"spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT\"")
ssh gpu1 /bin/bash -c "$CMD"
ssh gpu2 /bin/bash -c "$CMD"
ssh gpu3 /bin/bash -c "$CMD"
ssh gpu4 /bin/bash -c "$CMD"
