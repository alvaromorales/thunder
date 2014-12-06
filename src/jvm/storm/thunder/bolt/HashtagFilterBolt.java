package storm.thunder.bolt;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import storm.thunder.tools.HashtagFence;
import storm.thunder.util.TopologyFields;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HashtagFilterBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = -2120778103920585492L;
    private static final Logger LOG = Logger.getLogger(HashtagFilterBolt.class);

	private static final Pattern HASHTAG_PATTERN = Pattern.compile("(?:(?<=\\s)|^)#(\\w*[A-Za-z_]+\\w*)");
	private static final int HASHTAG_GROUP = 0;

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String id = (String) tuple.getValueByField(TopologyFields.ID_FIELD);
		String type = (String) tuple.getValueByField(TopologyFields.TYPE_FIELD);
		String tweet = (String) tuple.getValueByField(TopologyFields.TWEET_FIELD);
		
		Set<String> hashtags = getHashtags(tweet);
		LOG.info("Number of hashtags: " + hashtags.size());
		
		for (String hashtag : hashtags) {
			collector.emit(new Values(new HashtagFence(id, hashtag), id, type, hashtag));
		}
	}
	
	public static Set<String> getHashtags(String tweet) {
		Set<String> tags = Sets.newHashSet();
		
		if (tweet.contains("#")) {
			tweet = tweet.toLowerCase();
			Matcher m = HASHTAG_PATTERN.matcher(tweet);
			while (m.find()) {
				tags.add(m.group(HASHTAG_GROUP));
			}
		}

		return tags;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("obj", TopologyFields.ID_FIELD, TopologyFields.TYPE_FIELD, TopologyFields.HASHTAG_FIELD));
	}

}
