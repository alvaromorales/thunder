package storm.thunder.bolt;

import java.util.Set;

import com.google.common.collect.Sets;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TweetSplitterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -5434373599371326542L;

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String tweet = (String) tuple.getValueByField("tweet");
		Double lat = (Double) tuple.getValueByField("lat");
		Double lon = (Double) tuple.getValueByField("lon");
		
		Set<String> words = processTweet(tweet);
		collector.emit(new Values(words, lat, lon));
	}

	private Set<String> processTweet(String tweet) {
		tweet = tweet.toLowerCase();
		Set<String> words = Sets.newHashSet(tweet.split("[\\p{Punct}\\s+]"));
		words.remove("");
		return words;
	}

	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declare(new Fields("words", "lat", "lon"));
	}
	
}
