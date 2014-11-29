package storm.thunder.bolt;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import storm.thunder.tools.TopicMap;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TopicFilterBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = -6512257401799080975L;
	private TopicMap topics;
	private static final Logger LOG = Logger.getLogger(TopicFilterBolt.class);
	
	public TopicFilterBolt() {
		this.topics = new TopicMap();
		reloadTopics();
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		if (isTickTuple(tuple)) {
			reloadTopics();
		} else {
			@SuppressWarnings("unchecked")
			Set<String> words = (Set<String>) tuple.getValueByField("words");
			Double lat = (Double) tuple.getValueByField("lat");
			Double lon = (Double) tuple.getValueByField("lon");
			
			Set<String> topicsMatched = Sets.newHashSet();
			for (String word : words) {
				topicsMatched.addAll(topics.getMatchingTopics(word));
			}
			
			for (String topic : topicsMatched) {
				collector.emit(new Values(topic, lat, lon));
			}
		}
	}
	
	public void reloadTopics() {
		Set<String> homeworkWords = Sets.newHashSet("homework", "tarea");
		topics.addTopic("homework", homeworkWords);

		Set<String> trafficWords = Sets.newHashSet("traffic", "trafico");
		topics.addTopic("traffic", trafficWords);

		Set<String> flyingWords = Sets.newHashSet("landed", "aterrice");
		topics.addTopic("flying", flyingWords);

		LOG.debug("Reloaded topics: " + topics.getTopics());
		//TODO read topics from db, reload
	}

	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declare(new Fields("topic", "lat", "lon"));
	}

	private static boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		int tickFrequencyInSeconds = 10;
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
		return conf;
	}

}
