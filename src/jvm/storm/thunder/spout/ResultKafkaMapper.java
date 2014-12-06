package storm.thunder.spout;

import java.util.List;

import org.json.simple.JSONObject;

import com.google.common.collect.Lists;

import backtype.storm.tuple.Tuple;
import storm.kafka.bolt.mapper.TupleToKafkaMapper;
import storm.thunder.tools.HashtagFence;
import storm.thunder.tools.Rankable;
import storm.thunder.tools.Rankings;

public class ResultKafkaMapper implements TupleToKafkaMapper<String, String> {

	private static final long serialVersionUID = -3762079609639764045L;

	@Override
	public String getKeyFromTuple(Tuple tuple) {
		return "key";
	}

	@Override
	public String getMessageFromTuple(Tuple tuple) {
		Rankings rankings = (Rankings) tuple.getValueByField("rankings");
		List<Rankable> trends = rankings.getRankings();
		
		JSONObject result = new JSONObject();
		List<String> hashtags = Lists.newArrayList();
		String fenceId = null;
		String type = null;
		
		for (Rankable trend : trends) {
			HashtagFence obj = (HashtagFence) trend.getObject();
			if (fenceId == null) {
				fenceId = obj.getFenceId();
				type = obj.getType();
			}
			hashtags.add(obj.getHashtag());
		}
		
		result.put("feature", type);
		result.put("trends", hashtags);
		
		JSONObject finalResult = new JSONObject();
		finalResult.put(fenceId, result);
		
		return finalResult.toJSONString();
	}

}
