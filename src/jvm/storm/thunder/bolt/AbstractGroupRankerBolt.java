package storm.thunder.bolt;

import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import storm.thunder.tools.GroupedRankings;
import storm.thunder.util.TupleHelpers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This abstract bolt provides the basic behavior of bolts that rank objects according to their count and group.
 * <p/>
 * It uses a template method design pattern for {@link AbstractRankerBolt#execute(Tuple, BasicOutputCollector)} to allow
 * actual bolt implementations to specify how incoming tuples are processed, i.e. how the objects embedded within those
 * tuples are retrieved and counted.
 * 
 * Forked from storm.starter.bolt.AbstractRankerBolt from the Apache Storm examples
 * Not entirely our code!
 * See https://github.com/apache/storm/tree/master/examples/storm-starter
 * @author miguno - Michael G.Noll (original), alvaromorales - Alvaro Morales
 * See http://www.michael-noll.com/blog/2013/01/18/implementing-real-time-trending-topics-in-storm/
 */
public abstract class AbstractGroupRankerBolt extends AbstractFenceBolt {

	private static final long serialVersionUID = 4931640198501530202L;
	
	private static final Logger LOG = Logger.getLogger(AbstractGroupRankerBolt.class);
	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
	private static final int DEFAULT_COUNT = 10;
	private static final int DEFAULT_CLEANUP_FREQUENCY_IN_SECONDS = 60;

	private final int emitFrequencyInSeconds;
	private final int cleanupFrequencyInSeconds;
	private final int count;
	private final GroupedRankings rankings;

	private int cleanupCounter;

	public AbstractGroupRankerBolt() {
		this(DEFAULT_COUNT, DEFAULT_EMIT_FREQUENCY_IN_SECONDS, DEFAULT_CLEANUP_FREQUENCY_IN_SECONDS);
	}

	public AbstractGroupRankerBolt(int topN) {
		this(topN, DEFAULT_EMIT_FREQUENCY_IN_SECONDS, DEFAULT_CLEANUP_FREQUENCY_IN_SECONDS);
	}

	public AbstractGroupRankerBolt(int topN, int emitFrequencyInSeconds, int cleanupFrequencyInSeconds) {
		if (topN < 1) {
			throw new IllegalArgumentException("topN must be >= 1 (you requested " + topN + ")");
		}
		if (emitFrequencyInSeconds < 1) {
			throw new IllegalArgumentException(
					"The emit frequency must be >= 1 seconds (you requested " + emitFrequencyInSeconds + " seconds)");
		}
		count = topN;
		this.emitFrequencyInSeconds = emitFrequencyInSeconds;
		this.cleanupFrequencyInSeconds = cleanupFrequencyInSeconds;
		this.cleanupCounter = 0;
		rankings = new GroupedRankings(count);
	}

	protected GroupedRankings getRankings() {
		return rankings;
	}

	/**
	 * This method functions as a template method (design pattern).
	 */
	@Override
	public final void execute(Tuple tuple, BasicOutputCollector collector) {
		if (TupleHelpers.isTickTuple(tuple)) {
			cleanupCounter += emitFrequencyInSeconds;
			if (cleanupCounter > cleanupFrequencyInSeconds) {
				updateFences();
				cleanupFences();
				cleanupCounter = 0;
			}
			
			getLogger().debug("Received tick tuple, triggering emit of current rankings");
			emitRankings(collector);
		}
		else {
			updateRankingsWithTuple(tuple);
		}
	}

	public void cleanupFences() {
		ImmutableSet<Object> groups = rankings.getRankings().keySet();
		List<Object> toRemove = Lists.newArrayList();
		
		for (Object group : groups) {
			String fenceId = (String) group;
			if (!hasFence(fenceId)) {
				toRemove.add(group);
			}
		}
		
		for (Object group : toRemove) {
			LOG.info("Deleting group \"" + group.toString() + "\" from rankings");
			rankings.deleteGroup(group);
		}
	}

	abstract void updateRankingsWithTuple(Tuple tuple);

	private void emitRankings(BasicOutputCollector collector) {
		collector.emit(new Values(rankings.copy()));
		getLogger().debug("Rankings: " + rankings);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rankings"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
		return conf;
	}

	abstract Logger getLogger();
}