#!/bin/bash

export SPARK_HOME=/home/tqg432/simba/Simba/engine

./sbin/stop-master.sh

CMD=$(echo "cd . && cd $SPARK_HOME; \"sbin/stop-slave.sh\"")
ssh gpu1 /bin/bash -c "$CMD"
ssh gpu2 /bin/bash -c "$CMD"
ssh gpu3 /bin/bash -c "$CMD"
ssh gpu4 /bin/bash -c "$CMD"

