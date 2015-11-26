package net.iponweb;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.json.JSONArray;
import org.json.JSONObject;
import scala.collection.*;
import scala.collection.Map;



import java.io.FileWriter;


import java.io.IOException;
import java.util.*;

public class KafkaReplicaSetter {

    private static class CliArguments {
        @Parameter(names = "--topic", variableArity = true, description = "Set topic")
        public ArrayList<String> topics = new ArrayList<String>();

        @Parameter(names = "--zk", description = "Zookeeper host. Default: localhost/kafka")
        public String zk_host = "localhost/kafka";

        @Parameter(names = "--out", description = "Output json file")
        public String output_file = "ra.json";

        @Parameter(names = "--replica-factor", description = "Set replica factor. Default: 2")
        public int replica_factor = 2;

        @Parameter(names = {"-help", "-h", "--help"}, help = true, description = "Print this help message and exit")
        public boolean help;
    }

    public static void main(String[] args) throws Exception{

        CliArguments arguments = new CliArguments();
        JCommander commander = new JCommander(arguments, args);
        commander.setProgramName("java -jar KafkaReplicaSetter.jar");
        if (arguments.help) {
            commander.usage();
            System.exit(0);
        }

        KafkaTopicHelper kt = new KafkaTopicHelper(arguments.topics, arguments.replica_factor, arguments.zk_host);

        JSONArray jarr = new JSONArray();
        for(KafkaPartition x: kt.FixReplicaFactor()){
            jarr.put(new JSONObject(x.toJSONString()));
        }
        JSONObject joutput = new JSONObject();
        joutput.put("version", 1);
        joutput.put("partitions", jarr);
        System.out.println(joutput.toString());

        FileWriter file = new FileWriter(arguments.output_file);
        try {
            file.write(joutput.toString());
            System.out.println("Successfully Copied JSON Object to File...");
        } catch (IOException e) {
            e.printStackTrace();

        } finally {
            file.flush();
            file.close();
        }

    }

    public static class KafkaPartition {
        public String topic_name;
        public Integer partition_id;
        public ArrayList<Integer> replica_ids;

        public KafkaPartition(String topic_name, Integer partition_id, ArrayList<Integer> replica_ids){
            this.topic_name = topic_name;
            this.partition_id = partition_id;
            this.replica_ids = replica_ids;
        }

        public String toJSONString(){

            String str = "{\"topic\": \""+ topic_name + "\",\"partition\": " + partition_id + ",\"replicas\": " + replica_ids + "}";
            return str;
        }

    }

    public static class KafkaTopicHelper {

        public int globalReplicaFactor;
        private ZkClient zkClient;
        public ArrayList<Integer> broker_ids;
        public ArrayList<String> topics;



        public KafkaTopicHelper(ArrayList<String> topics, int replicaFactor, String zk_conn_string){
            this.topics = topics;
            this.globalReplicaFactor = replicaFactor;
            this.zkClient = new ZkClient(zk_conn_string, 1000, 1000, ZKStringSerializer$.MODULE$);
            this.broker_ids = getBrokerList();
        }

        public ArrayList<Integer> getBrokerList() {
            List<String> broker_list = zkClient.getChildren(ZkUtils.BrokerIdsPath());
            ArrayList<Integer> broker_ids = new ArrayList<Integer>();
            for (String id: broker_list){
                broker_ids.add(Integer.parseInt(id));

            }
            return broker_ids;
        }

        public ArrayList<KafkaPartition> GetTopicMap(){
            //Doing some scala-java magic... Looks like it's shit.
            ArrayList<KafkaPartition> partitions = new ArrayList<KafkaPartition>();

            Seq<String> topicList;
            if (topics.get(0).equals("all")){
                topicList = ZkUtils.getAllTopics(zkClient);
            } else {
                topicList = JavaConversions.asScalaBuffer(topics);
            }

            Map<String, Map<Object, Seq<Object>>> partitionAssignment = ZkUtils.getPartitionAssignmentForTopics(zkClient, topicList);

            for(java.util.Map.Entry<String, Map<Object, Seq<Object>>> topic_map : JavaConversions.mapAsJavaMap(partitionAssignment).entrySet()){
                String topic_name = topic_map.getKey();
                java.util.Map<Object, Seq<Object>> value = JavaConversions.mapAsJavaMap(topic_map.getValue());
                for(java.util.Map.Entry<Object, Seq<Object>> partition_map : value.entrySet() ){
                    Object partition_id = partition_map.getKey();
                    List<Object> partition_replicas = JavaConversions.asJavaList(partition_map.getValue());
                    ArrayList<Integer> brokers_list = new ArrayList<Integer>();
                    for(Object x : partition_replicas){
                        brokers_list.add(Integer.parseInt(x.toString()));
                    }
                    partitions.add(new KafkaPartition(topic_name, Integer.parseInt(partition_id.toString()), brokers_list));
                }
            }
            return partitions;
        }

        public ArrayList<KafkaPartition> FixReplicaFactor(){
            ArrayList<KafkaPartition> partitions = GetTopicMap();
            for(KafkaPartition p : partitions){
                AddRemoveBrokers(p);
            }
            return partitions;
        }

        private void AddRemoveBrokers (KafkaPartition p){
            if ( p.replica_ids.size() > globalReplicaFactor ){
                while (p.replica_ids.size() != globalReplicaFactor){
                    p.replica_ids.remove(new Random().nextInt(p.replica_ids.size()));
                }
            }
            if ( p.replica_ids.size() < globalReplicaFactor ){
                while (p.replica_ids.size() != globalReplicaFactor) {
                    Integer id = broker_ids.get(new Random().nextInt(broker_ids.size()));
                    if(p.replica_ids.contains(id))
                        continue;
                    p.replica_ids.add(id);
                }
            }
        }

    }


}
