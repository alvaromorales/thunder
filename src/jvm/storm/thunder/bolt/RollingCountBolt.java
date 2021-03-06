/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.thunder.bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import storm.thunder.spout.MessagesScheme;
import storm.thunder.tools.FenceStat;
import storm.thunder.tools.NthLastModifiedTimeTracker;
import storm.thunder.tools.SlidingWindowCounter;
import storm.thunder.util.TupleHelpers;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

/**
 * This bolt performs rolling counts of incoming objects, i.e. sliding window based counting.
 * <p/>
 * The bolt is configured by two parameters, the length of the sliding window in seconds (which influences the output
 * data of the bolt, i.e. how it will count objects) and the emit frequency in seconds (which influences how often the
 * bolt will output the latest window counts). For instance, if the window length is set to an equivalent of five
 * minutes and the emit frequency to one minute, then the bolt will output the latest five-minute sliding window every
 * minute.
 * <p/>
 * The bolt emits a rolling count tuple per object, consisting of the object itself, its latest rolling count, and the
 * actual duration of the sliding window. The latter is included in case the expected sliding window length (as
 * configured by the user) is different from the actual length, e.g. due to high system load. Note that the actual
 * window length is tracked and calculated for the window, and not individually for each object within a window.
 * <p/>
 * Note: During the startup phase you will usually observe that the bolt warns you about the actual sliding window
 * length being smaller than the expected length. This behavior is expected and is caused by the way the sliding window
 * counts are initially "loaded up". You can safely ignore this warning during startup (e.g. you will see this warning
 * during the first ~ five minutes of startup time if the window length is set to five minutes).
 * 
 * From the Apache Storm examples
 * Not our code!
 * See https://github.com/apache/storm/tree/master/examples/storm-starter
 * @author miguno - Michael G.Noll
 * See http://www.michael-noll.com/blog/2013/01/18/implementing-real-time-trending-topics-in-storm/
 */
public class RollingCountBolt extends AbstractFenceBolt {

	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOG = Logger.getLogger(RollingCountBolt.class);
//	private static final int NUM_WINDOW_CHUNKS = 60;
	private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = 3600; //NUM_WINDOW_CHUNKS * 60;
	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 10; //DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
	private static final int DEFAULT_CLEANUP_FREQUENCY_IN_SECONDS = 120;
	private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
			"Actual window length is %d seconds when it should be %d seconds"
					+ " (you can safely ignore this warning during the startup phase)";

	private final SlidingWindowCounter<Object> counter;
	private final int windowLengthInSeconds;
	private final int emitFrequencyInSeconds;
	private final int cleanupFrequencyInSeconds;
	private NthLastModifiedTimeTracker lastModifiedTracker;

	private int cleanupCounter;

	public RollingCountBolt() {
		this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS, DEFAULT_CLEANUP_FREQUENCY_IN_SECONDS);
	}

	public RollingCountBolt(int windowLengthInSeconds, int emitFrequencyInSeconds, int cleanupFrequencyInSeconds) {
		this.windowLengthInSeconds = windowLengthInSeconds;

		if (cleanupFrequencyInSeconds < emitFrequencyInSeconds) {
			throw new IllegalArgumentException("Cleanup frequency must be greater than the emit frequency");
		}
		this.emitFrequencyInSeconds = emitFrequencyInSeconds;
		this.cleanupFrequencyInSeconds = cleanupFrequencyInSeconds;
		this.cleanupCounter = 0;

		counter = new SlidingWindowCounter<Object>(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
				this.emitFrequencyInSeconds));
	}

	private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
		return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
		lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
				this.emitFrequencyInSeconds));
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		if (TupleHelpers.isTickTuple(tuple)) {
			LOG.debug("Received tick tuple, triggering emit of current window counts");

			cleanupCounter += emitFrequencyInSeconds;
			if (cleanupCounter > cleanupFrequencyInSeconds) {
				updateFences();
				cleanupFences();
				cleanupCounter = 0;
			}

			emitCurrentWindowCounts(collector);
		} else {
			countObjAndAck(tuple, collector);
		}
	}

	public void cleanupFences() {
		Map<Object, Long> counts = counter.getCounts();
		List<Object> toRemove = Lists.newArrayList();

		for (Object o : counts.keySet()) {
			FenceStat st = (FenceStat) o;

			if (st.getFenceId().equals(MessagesScheme.TOTAL_COUNT_FIELD)) {
				continue;
			}

			if (!hasFence(st.getFenceId())) {
				LOG.debug("Removing stat \"" + st + "\" from counter since fence " + st.getFenceId() + " was deleted.");
				toRemove.add(o);
			}
		}

		counter.wipeObjects(toRemove);
	}

	private void emitCurrentWindowCounts(BasicOutputCollector collector) {
		Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
		int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
		lastModifiedTracker.markAsModified();
		if (actualWindowLengthInSeconds != windowLengthInSeconds) {
			LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
		}
		emit(counts, actualWindowLengthInSeconds, collector);
	}

	private void emit(Map<Object, Long> counts, int actualWindowLengthInSeconds, BasicOutputCollector collector) {
		for (Entry<Object, Long> entry : counts.entrySet()) {
			FenceStat obj = (FenceStat) entry.getKey();
			Long count = entry.getValue();
			String group = obj.getFenceId();
			collector.emit(new Values(obj, count, group, actualWindowLengthInSeconds));
		}
	}

	private void countObjAndAck(Tuple tuple, BasicOutputCollector collector) {
		Object obj = tuple.getValue(0);
		counter.incrementCount(obj);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("obj", "count", "group", "actualWindowLengthInSeconds"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
		return conf;
	}
}