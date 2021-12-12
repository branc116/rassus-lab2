set -ex
if [[ $USER != "kafka" ]]
then
    echo "ERROR: Start this script with kafka user"
else
    /home/kafka/bin/zookeeper-server-start.sh /home/kafka/config/zookeeper.properties &
    /home/kafka/bin/kafka-server-start.sh /home/kafka/config/server.properties
fi