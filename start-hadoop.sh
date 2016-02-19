#!bin/bash
export HADOOP_HOME=/usr/local/hadoop 
echo "---------------------Starting HADOOP DFS--------------------------"
. $HADOOP_HOME/sbin/start-dfs.sh

echo "---------------------Starting YARN-----------------------------------"
. $HADOOP_HOME/sbin/start-yarn.sh

