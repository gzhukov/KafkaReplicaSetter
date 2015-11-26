# KafkaReplicaSetter
This program could fix a wrong replica-factor on kafka cluster

It generates a reassignment configuration to move the partitions of the specified topics to the list of brokers (from zookeeper)

Usage: java -jar KafkaReplicaSetter.jar [options]  
  Options:  
    -help, -h, --help  
       Print this help message and exit  
       Default: false  
    --out  
       Output json file  
       Default: ra.json  
    --replica-factor  
       Set replica factor. Default: 2  
       Default: 2  
    --topic  
       Set topic  
       Default: []  
    --zk  
       Zookeeper host. Default: localhost/kafka  
       Default: localhost/kafka  
       

Then copy output file to one of kafka brokers and run comman:  
/usr/lib/kafka/bin/kafka-reassign-partitions.sh --zookeeper <zk-host>:<zk-port>/kafka --reassignment-json-file <output-json-file>  --execute
