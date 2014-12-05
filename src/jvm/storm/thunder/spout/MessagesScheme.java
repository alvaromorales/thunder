package storm.thunder.spout;

import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

public class MessagesScheme implements Scheme {
	
	public static final String TREND_FEATURE = "trend";
	public static final String COUNT_FEATURE = "count";
	public static final String TOPIC_FEATURE = "topic";

	private static final long serialVersionUID = -3037004756189554023L;

	public List<Object> deserialize(byte[] ser) {
		// TODO Auto-generated method stub
		return null;
	}

	public Fields getOutputFields() {
		// TODO Auto-generated method stub
		return null;
	}

}
