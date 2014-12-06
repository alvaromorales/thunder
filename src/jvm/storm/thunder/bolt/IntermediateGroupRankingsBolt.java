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

import storm.thunder.tools.GroupRankable;
import storm.thunder.tools.RankableObjectWithGroupAndFields;

/**
 * This bolt ranks incoming objects by their count.
 * <p/>
 * It assumes the input tuples to adhere to the following format: (object, object_count, additionalField1,
 * additionalField2, ..., additionalFieldN).
 * 
 * From the Apache Storm examples
 * Not our code!
 * See https://github.com/apache/storm/tree/master/examples/storm-starter
 * @author miguno - Michael G.Noll
 * See http://www.michael-noll.com/blog/2013/01/18/implementing-real-time-trending-topics-in-storm/
 */
public final class IntermediateGroupRankingsBolt extends AbstractGroupRankerBolt {

  private static final long serialVersionUID = -1369800530256637409L;
  private static final Logger LOG = Logger.getLogger(IntermediateGroupRankingsBolt.class);

  public IntermediateGroupRankingsBolt() {
    super();
  }

  public IntermediateGroupRankingsBolt(int topN) {
    super(topN);
  }

  public IntermediateGroupRankingsBolt(int topN, int emitFrequencyInSeconds) {
    super(topN, emitFrequencyInSeconds);
  }

  @Override
  void updateRankingsWithTuple(Tuple tuple) {
    GroupRankable rankable = RankableObjectWithGroupAndFields.from(tuple);
    super.getRankings().updateWith(rankable, rankable.getGroup());
  }

  @Override
  Logger getLogger() {
    return LOG;
  }
}