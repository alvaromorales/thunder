package storm.thunder;

import storm.thunder.bolt.GeoFilterBolt;
import storm.thunder.bolt.RollingCountBolt;
import storm.thunder.bolt.TopicFilterBolt;
import storm.thunder.bolt.TweetSplitterBolt;
import storm.thunder.spout.TwitterKafkaSpout;
import storm.thunder.util.StormRunner;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class ThunderTopology {

	public static final String TOPOLOGY_NAME = "ThunderTopology";
	
	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("TwitterKafkaSpout", new TwitterKafkaSpout());
		builder.setBolt("TweetSplitterBolt", new TweetSplitterBolt()).shuffleGrouping("TwitterKafkaSpout");
		builder.setBolt("TopicFilterBolt", new TopicFilterBolt()).shuffleGrouping("TweetSplitterBolt");
		builder.setBolt("GeoFilterBolt", new GeoFilterBolt()).shuffleGrouping("TopicFilterBolt");
		builder.setBolt("RollingCountBolt", new RollingCountBolt()).globalGrouping("GeoFilterBolt");
		
		try {
			StormRunner.runTopologyLocally(builder.createTopology(), TOPOLOGY_NAME, config, 60 * 10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
