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

import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;

import storm.thunder.tools.GroupedRankings;

/**
 * This bolt merges incoming {@link Rankings}.
 * <p/>
 * It can be used to merge intermediate rankings generated by {@link IntermediateRankingsBolt} into a final,
 * consolidated ranking. To do so, configure this bolt with a globalGrouping on {@link IntermediateRankingsBolt}.
 * 
 * From the Apache Storm examples
 * Not our code!
 * See https://github.com/apache/storm/tree/master/examples/storm-starter
 * @author miguno - Michael G.Noll
 * See http://www.michael-noll.com/blog/2013/01/18/implementing-real-time-trending-topics-in-storm/
 */
public final class TotalGroupRankingsBolt extends AbstractGroupRankerBolt {

	private static final long serialVersionUID = -8447525895532302198L;
	private static final Logger LOG = Logger.getLogger(TotalGroupRankingsBolt.class);

	public TotalGroupRankingsBolt() {
		super();
	}

	public TotalGroupRankingsBolt(int topN) {
		super(topN);
	}

	public TotalGroupRankingsBolt(int topN, int emitFrequencyInSeconds, int cleanupFrequencyInSeconds) {
		super(topN, emitFrequencyInSeconds, cleanupFrequencyInSeconds);
	}

	@Override
	void updateRankingsWithTuple(Tuple tuple) {
		GroupedRankings rankingsToBeMerged = (GroupedRankings) tuple.getValue(0);
		super.getRankings().updateWith(rankingsToBeMerged);
		super.getRankings().pruneZeroCounts();
	}

	@Override
	Logger getLogger() {
		return LOG;
	}

}