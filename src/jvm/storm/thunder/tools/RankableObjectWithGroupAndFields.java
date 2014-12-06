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
package storm.thunder.tools;

import backtype.storm.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;

/**
 * This class wraps an objects, its group, and its associated count, including any additional data fields.
 * <p/>
 * This class can be used, for instance, to track the number of occurrences of an object in a Storm topology.
 * 
 * Forked from storm.starter.tools.RankableObjectWithFields in the Apache Storm examples
 * Not entirely our code!
 * See https://github.com/apache/storm/tree/master/examples/storm-starter
 * @author miguno - Michael G.Noll, alvaromorales - Alvaro Morales
 * See http://www.michael-noll.com/blog/2013/01/18/implementing-real-time-trending-topics-in-storm/
 */
public class RankableObjectWithGroupAndFields implements GroupRankable, Serializable {

  private static final long serialVersionUID = -9102878650001058090L;
  private static final String toStringSeparator = "|";

  private final Object obj;
  private final long count;
  private final Object group;
  private final ImmutableList<Object> fields;

  public RankableObjectWithGroupAndFields(Object obj, long count, Object group, Object... otherFields) {
    if (obj == null) {
      throw new IllegalArgumentException("The object must not be null");
    }
    if (count < 0) {
      throw new IllegalArgumentException("The count must be >= 0");
    }
    if (group == null) {
      throw new IllegalArgumentException("The group must not be null");
    }
    this.obj = obj;
    this.count = count;
    this.group = group;
    fields = ImmutableList.copyOf(otherFields);

  }

  /**
   * Construct a new instance based on the provided {@link Tuple}.
   * <p/>
   * This method expects the object to be ranked in the first field (index 0) of the provided tuple, the number of
   * occurrences of the object (its count) in the second field (index 1), and the group (index 2). Any further fields in the tuple will be
   * extracted and tracked, too. These fields can be accessed via {@link RankableObjectWithFields#getFields()}.
   *
   * @param tuple
   *
   * @return new instance based on the provided tuple
   */
  public static RankableObjectWithGroupAndFields from(Tuple tuple) {
    List<Object> otherFields = Lists.newArrayList(tuple.getValues());
    Object obj = otherFields.remove(0);
    Long count = (Long) otherFields.remove(0);
    Object group = otherFields.remove(0);
    return new RankableObjectWithGroupAndFields(obj, count, group, otherFields.toArray());
  }

  public Object getObject() {
    return obj;
  }

  public long getCount() {
    return count;
  }
  
  public Object getGroup() {
	  return group;
  }

  /**
   * @return an immutable list of any additional data fields of the object (may be empty but will never be null)
   */
  public List<Object> getFields() {
    return fields;
  }

  @Override
  public int compareTo(GroupRankable other) {
    long delta = this.getCount() - other.getCount();
    if (delta > 0) {
      return 1;
    }
    else if (delta < 0) {
      return -1;
    }
    else {
      return 0;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RankableObjectWithGroupAndFields)) {
      return false;
    }
    RankableObjectWithGroupAndFields other = (RankableObjectWithGroupAndFields) o;
    return obj.equals(other.obj) && count == other.count && group.equals(other.group);
  }

  @Override
  public int hashCode() {
    int result = 17;
    int countHash = (int) (count ^ (count >>> 32));
    result = 31 * result + countHash;
    result = 31 * result + obj.hashCode();
    result = 31 * result + group.hashCode();
    return result;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("[");
    buf.append(obj);
    buf.append(toStringSeparator);
    buf.append(count);
    for (Object field : fields) {
      buf.append(toStringSeparator);
      buf.append(field);
    }
    buf.append("]");
    return buf.toString();
  }

  /**
   * Note: We do not defensively copy the wrapped object and any accompanying fields.  We do guarantee, however,
   * do return a defensive (shallow) copy of the List object that is wrapping any accompanying fields.
   *
   * @return
   */
  @Override
  public GroupRankable copy() {
    List<Object> shallowCopyOfFields = ImmutableList.copyOf(getFields());
    return new RankableObjectWithGroupAndFields(getObject(), getCount(), getGroup(), shallowCopyOfFields);
  }

}