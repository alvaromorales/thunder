package storm.thunder.bolt;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import storm.thunder.spout.MessagesScheme;
import storm.thunder.tools.Count;
import storm.thunder.tools.GroupRankable;
import storm.thunder.tools.GroupedRankings;
import storm.thunder.tools.Hashtag;
import storm.thunder.util.TupleHelpers;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AggregateBolt extends AbstractFenceBolt {

	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
	private static final int DEFAULT_CLEANUP_FREQUENCY_IN_SECONDS = 60;

	private static final long serialVersionUID = -5583754954739361149L;

	private static final Logger LOG = Logger.getLogger(AggregateBolt.class);

	private int cleanupCounter;
	private JSONObject results;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.cleanupCounter = 0;
		this.results = new JSONObject();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tuple) {
		if (TupleHelpers.isTickTuple(tuple)) {
			LOG.debug("Received tick tuple, triggering emit of current results");

			cleanupCounter += DEFAULT_EMIT_FREQUENCY_IN_SECONDS;
			if (cleanupCounter > DEFAULT_CLEANUP_FREQUENCY_IN_SECONDS) {
				updateFences();
				cleanupFences();
				cleanupCounter = 0;
			}

			emitResults();
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
			} else if (tuple.getSourceComponent().equals("totalRollingCountBolt")) {
				Count c = (Count) tuple.getValue(0);
				JSONObject fenceResult = new JSONObject();
				fenceResult.put("feature", MessagesScheme.COUNT_FEATURE);
				fenceResult.put("count", c.getCount());
				results.put(c.getFenceId(), fenceResult);
			}
		}
	}

	private void emitResults() {
		collector.emit(new Values(results));
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
		int tickFrequencyInSeconds = 2;
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
		return conf;
	}

}
