package storm.thunder;

import java.util.Properties;
import java.util.UUID;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import storm.thunder.bolt.GeoFilterBolt;
import storm.thunder.bolt.HashtagFilterBolt;
import storm.thunder.bolt.IntermediateGroupRankingsBolt;
import storm.thunder.bolt.RollingCountBolt;
import storm.thunder.bolt.TotalGroupRankingsBolt;
import storm.thunder.spout.ResultKafkaMapper;
import storm.thunder.spout.TweetScheme;
import storm.thunder.util.StormRunner;
import backtype.storm.Config;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class ThunderTopology {

	public static final String TOPOLOGY_NAME = "ThunderTopology";
	public static final String KAFKA_TWEET_TOPIC = "tweets";
	public static final String KAFKA_OUTPUT_TOPIC = "results";
	
	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		TopologyBuilder builder = new TopologyBuilder();
		
		// Kafka Spout
		ZkHosts zkHosts = new ZkHosts("localhost:2181");
		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, KAFKA_TWEET_TOPIC, "/kafkastorm", UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new TweetScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		builder.setSpout("kafkaSpout", kafkaSpout);

		// Bolts
		builder.setBolt("geoFilterBolt", new GeoFilterBolt())
			.shuffleGrouping("kafkaSpout");

		builder.setBolt("hashtagFilterBolt", new HashtagFilterBolt())
			.shuffleGrouping("geoFilterBolt");

		builder.setBolt("rollingCountBolt", new RollingCountBolt())
			.fieldsGrouping("hashtagFilterBolt", new Fields("obj"));
		
		builder.setBolt("intermediateGroupRankingsBolt", new IntermediateGroupRankingsBolt(10), 2)
			.fieldsGrouping("rollingCountBolt", new Fields("obj"));
		
		builder.setBolt("totalGroupRankingsBolt", new TotalGroupRankingsBolt(10))
        	.globalGrouping("intermediateGroupRankingsBolt");
		
		// Kafka Output Bolt
		@SuppressWarnings({ "rawtypes", "unchecked" })
		KafkaBolt kafkaBolt = new KafkaBolt()
				.withTopicSelector(new DefaultTopicSelector(KAFKA_OUTPUT_TOPIC))
				.withTupleToKafkaMapper(new ResultKafkaMapper());
		builder.setBolt("kafkaBolt", kafkaBolt).globalGrouping("totalGroupRankingsBolt");
		
		Properties props = new Properties();
		  props.put("metadata.broker.list", "localhost:9092");
		  props.put("request.required.acks", "1");
		  props.put("serializer.class", "kafka.serializer.StringEncoder");
		  config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
		
		try {
			StormRunner.runTopologyLocally(builder.createTopology(), TOPOLOGY_NAME, config, 60 * 10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
