package storm.thunder.spout;

import org.json.simple.JSONObject;

import backtype.storm.tuple.Tuple;
import storm.kafka.bolt.mapper.TupleToKafkaMapper;

public class ResultKafkaMapper implements TupleToKafkaMapper<String, String> {

	private static final long serialVersionUID = -3762079609639764045L;

	@Override
	public String getKeyFromTuple(Tuple tuple) {
		return "key";
	}

	@Override
	public String getMessageFromTuple(Tuple tuple) {
		JSONObject results = (JSONObject) tuple.getValueByField("results");
		return results.toJSONString();
	}

}
