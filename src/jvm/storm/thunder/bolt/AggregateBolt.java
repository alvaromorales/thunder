package storm.thunder.bolt;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import storm.thunder.spout.MessagesScheme;
import storm.thunder.tools.Count;
import storm.thunder.tools.GroupRankable;
import storm.thunder.tools.GroupedRankings;
import storm.thunder.tools.Hashtag;
import storm.thunder.util.TupleHelpers;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class AggregateBolt extends AbstractFenceBolt {

	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 10;
	private static final int DEFAULT_CLEANUP_FREQUENCY_IN_SECONDS = 60;

	private static final long serialVersionUID = -5583754954739361149L;

	private static final Logger LOG = Logger.getLogger(AggregateBolt.class);

	private int cleanupCounter;
	private JSONObject results;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
		this.cleanupCounter = 0;
		this.results = new JSONObject();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		if (TupleHelpers.isTickTuple(tuple)) {
			LOG.debug("Received tick tuple, triggering emit of current results");

			cleanupCounter += DEFAULT_EMIT_FREQUENCY_IN_SECONDS;
			if (cleanupCounter > DEFAULT_CLEANUP_FREQUENCY_IN_SECONDS) {
				updateFences();
				cleanupFences();
				cleanupCounter = 0;
			}

			emitResults(collector);
		} else {
			if (tuple.getSourceComponent().equals("totalGroupRankingsBolt")) {
				GroupedRankings gr = (GroupedRankings) tuple.getValueByField("rankings");
				ImmutableMap<Object, List<GroupRankable>> rankings = gr.getRankings();

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
					results.put(fenceId, fenceResult);
				}
			} else if (tuple.getSourceComponent().equals("countRollingCountBolt")) {
				JSONObject fenceResult = new JSONObject();
				Count c = (Count) tuple.getValue(0);
				long count = (Long) tuple.getValue(1);
				fenceResult.put("feature", MessagesScheme.COUNT_FEATURE);
				fenceResult.put("count", count);
				results.put(c.getFenceId(), fenceResult);
			}
		}
	}

	private void emitResults(BasicOutputCollector collector) {
		if (!results.isEmpty()) {
			collector.emit(new Values(results));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("results"));
	}

	@Override
	public void cleanupFences() {
		List<Object> toRemove = Lists.newArrayList();
		for (Object key : results.keySet()) {
			String fenceId = (String) key;
			
			if (fenceId.equals(MessagesScheme.TOTAL_COUNT_FIELD)) {
				continue;
			}
			
			if (!hasFence(fenceId)) {
				toRemove.add(key);
			}
		}
		
		for (Object key : toRemove) {
			results.remove(key);
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		int tickFrequencyInSeconds = 10;
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
		return conf;
	}

}
