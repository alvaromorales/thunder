package storm.thunder;

import java.util.UUID;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.thunder.bolt.GeoFilterBolt;
import storm.thunder.bolt.RollingCountBolt;
import storm.thunder.bolt.TopicFilterBolt;
import storm.thunder.bolt.TweetSplitterBolt;
import storm.thunder.spout.TweetScheme;
import storm.thunder.util.StormRunner;
import backtype.storm.Config;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

public class ThunderTopology {

	public static final String TOPOLOGY_NAME = "ThunderTopology";
	public static final String KAFKA_TOPIC = "tweets";
	
	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		TopologyBuilder builder = new TopologyBuilder();
		
		// Kafka Spout
		ZkHosts zkHosts = new ZkHosts("localhost:2181");
		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, KAFKA_TOPIC, "/kafkastorm", UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new TweetScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		builder.setSpout("KafkaSpout", kafkaSpout);

		// Bolts
		
		//TODO need to deserialize message in first bolt?
		
		builder.setBolt("TweetSplitterBolt", new TweetSplitterBolt()).shuffleGrouping("KafkaSpout");
		builder.setBolt("TopicFilterBolt", new TopicFilterBolt()).shuffleGrouping("TweetSplitterBolt");
		builder.setBolt("GeoFilterBolt", new GeoFilterBolt()).shuffleGrouping("TopicFilterBolt");
		builder.setBolt("RollingCountBolt", new RollingCountBolt()).globalGrouping("GeoFilterBolt");
		
		// Kafka Sink
		// TODO
		
		try {
			StormRunner.runTopologyLocally(builder.createTopology(), TOPOLOGY_NAME, config, 60 * 10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
