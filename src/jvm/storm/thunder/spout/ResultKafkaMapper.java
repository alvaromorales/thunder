package storm.thunder.spout;

import java.util.List;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import backtype.storm.tuple.Tuple;
import storm.kafka.bolt.mapper.TupleToKafkaMapper;
import storm.thunder.tools.GroupRankable;
import storm.thunder.tools.GroupedRankings;
import storm.thunder.tools.Hashtag;

public class ResultKafkaMapper implements TupleToKafkaMapper<String, String> {

	private static final long serialVersionUID = -3762079609639764045L;
	private static final Logger LOG = Logger.getLogger(ResultKafkaMapper.class);

	@Override
	public String getKeyFromTuple(Tuple tuple) {
		return "key";
	}

	@Override
	public String getMessageFromTuple(Tuple tuple) {
		GroupedRankings gr = (GroupedRankings) tuple.getValueByField("rankings");
		JSONObject result = new JSONObject();
		
		ImmutableMap<Object, List<GroupRankable>> rankings = gr.getRankings();
		LOG.debug("Received rankings with the following " + rankings.size() + " group(s): " + rankings.keySet());
		
		for (Object group : rankings.keySet()) {
			String fenceId = (String) group;
			JSONObject fenceResult = new JSONObject();
			
			List<GroupRankable> trends = rankings.get(group);
			List<String> hashtags = Lists.newArrayList();

			for (GroupRankable trend : trends) {
				Hashtag obj = (Hashtag) trend.getObject();
				hashtags.add(obj.getHashtag());
			}

			fenceResult.put("feature", MessagesScheme.TREND_FEATURE);
			fenceResult.put("trends", hashtags);
			result.put(fenceId, fenceResult);
		}

		return result.toJSONString();
	}

}
