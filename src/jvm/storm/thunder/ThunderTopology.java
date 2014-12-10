package storm.thunder;

import java.util.Properties;
import java.util.UUID;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import storm.thunder.bolt.AggregateBolt;
import storm.thunder.bolt.GeoFilterBolt;
import storm.thunder.bolt.HashtagFilterBolt;
import storm.thunder.bolt.IntermediateGroupRankingsBolt;
import storm.thunder.bolt.RollingCountBolt;
import storm.thunder.bolt.TotalGroupRankingsBolt;
import storm.thunder.spout.MessagesScheme;
import storm.thunder.spout.ResultKafkaMapper;
import storm.thunder.spout.TweetScheme;
import storm.thunder.util.TopologyFields;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class ThunderTopology {

	public static final String TOPOLOGY_NAME = "ThunderTopology";
	public static final String KAFKA_TWEET_TOPIC = "tweets";
	public static final String KAFKA_OUTPUT_TOPIC = "results";

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		TopologyBuilder builder = new TopologyBuilder();

		// Kafka Spout
		ZkHosts zkHosts = new ZkHosts("zookeeper1.storm.tweetfence.com:2181");
		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, KAFKA_TWEET_TOPIC, "/kafkastorm", UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new TweetScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		builder.setSpout("kafkaSpout", kafkaSpout);

		// Geo filter bolt
		builder.setBolt("geoFilterBolt", new GeoFilterBolt(), 3)
			.shuffleGrouping("kafkaSpout");

		// Trending topology branch
		builder.setBolt("hashtagFilterBolt", new HashtagFilterBolt())
			.shuffleGrouping("geoFilterBolt", MessagesScheme.TREND_FEATURE);

		builder.setBolt("rollingCountBolt", new RollingCountBolt())
			.fieldsGrouping("hashtagFilterBolt", new Fields("obj"));

		builder.setBolt("intermediateGroupRankingsBolt", new IntermediateGroupRankingsBolt(10), 2)
			.fieldsGrouping("rollingCountBolt", new Fields("obj"));

		builder.setBolt("totalGroupRankingsBolt", new TotalGroupRankingsBolt(10))
			.globalGrouping("intermediateGroupRankingsBolt");

		// Counting topology branch
		builder.setBolt("countRollingCountBolt", new RollingCountBolt(), 3)
			.fieldsGrouping("geoFilterBolt", MessagesScheme.COUNT_FEATURE, new Fields(TopologyFields.COUNT_FIELD))
			.fieldsGrouping("geoFilterBolt", MessagesScheme.TOTAL_FEATURE, new Fields(TopologyFields.COUNT_FIELD));

		// Aggregate bolt
		builder.setBolt("aggregateBolt", new AggregateBolt())
			.globalGrouping("totalGroupRankingsBolt")
			.globalGrouping("countRollingCountBolt");

		// Kafka Output Bolt
		@SuppressWarnings({ "rawtypes", "unchecked" })
		KafkaBolt kafkaBolt = new KafkaBolt()
			.withTopicSelector(new DefaultTopicSelector(KAFKA_OUTPUT_TOPIC))
			.withTupleToKafkaMapper(new ResultKafkaMapper());

		builder.setBolt("kafkaBolt", kafkaBolt)
			.globalGrouping("aggregateBolt");

		Properties props = new Properties();
		props.put("metadata.broker.list", "kafka1.storm.tweetfence.com:9092");
		props.put("request.required.acks", "1");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);

		if (args != null && args.length > 0) {
			config.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], config, builder.createTopology());
		}
		else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", config, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}
