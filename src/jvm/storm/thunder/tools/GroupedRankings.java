package storm.thunder.tools;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

/**
 * From from storm.starter.tools.Rankings in the Apache Storm examples
 * Not entirely our code!
 * See https://github.com/apache/storm/tree/master/examples/storm-starter
 * @author miguno - Michael G.Noll
 * See http://www.michael-noll.com/blog/2013/01/18/implementing-real-time-trending-topics-in-storm/
 **/
public class GroupedRankings implements Serializable {

	private static final long serialVersionUID = -1549827195410578903L;
	private static final int DEFAULT_COUNT = 10;
    private static final Logger LOG = Logger.getLogger(GroupedRankings.class);

	private final int maxSize;
	private final Map<Object, List<GroupRankable>> rankedItems = Maps.newHashMap();

	public GroupedRankings() {
		this(DEFAULT_COUNT);
	}

	public GroupedRankings(int topN) {
		if (topN < 1) {
			throw new IllegalArgumentException("topN must be >= 1");
		}
		maxSize = topN;
	}

	/**
	 * Copy constructor.
	 * @param other
	 */
	public GroupedRankings(GroupedRankings other) {
		this(other.maxSize());
		updateWith(other);
	}

	/**
	 * @return the maximum possible number (size) of ranked objects this instance can hold
	 */
	public int maxSize() {
		return maxSize;
	}

	/**
	 * @return the number (size) of ranked objects this instance is currently holding
	 */
	public int size() {
		return rankedItems.size();
	}

	/**
	 * The returned defensive copy is only "somewhat" defensive.  We do, for instance, return a defensive copy of the
	 * enclosing List instance, and we do try to defensively copy any contained GroupRankable objects, too.  However, the
	 * contract of {@link storm.starter.tools.GroupRankable#copy()} does not guarantee that any Object's embedded within
	 * a GroupRankable will be defensively copied, too.
	 *
	 * @return a somewhat defensive copy of ranked items
	 */
	public ImmutableMap<Object, List<GroupRankable>> getRankings() {
		Map<Object, List<GroupRankable>> rankingsCopy = Maps.newHashMap();
		for (Object group : rankedItems.keySet()) {
			List<GroupRankable> copy = Lists.newLinkedList();
			for (GroupRankable r: rankedItems.get(group)) {
				copy.add(r.copy());
			}
			rankingsCopy.put(group, copy);
		}
		return ImmutableMap.copyOf(rankingsCopy);
	}

	/**
	 * The returned defensive copy is only "somewhat" defensive.  We do, for instance, return a defensive copy of the
	 * enclosing List instance, and we do try to defensively copy any contained GroupRankable objects, too.  However, the
	 * contract of {@link storm.starter.tools.GroupRankable#copy()} does not guarantee that any Object's embedded within
	 * a GroupRankable will be defensively copied, too.
	 *
	 * @return a somewhat defensive copy of ranked items
	 */
	public List<GroupRankable> getRankings(Object group) {
		List<GroupRankable> copy = Lists.newLinkedList();
		for (GroupRankable r: rankedItems.get(group)) {
			copy.add(r.copy());
		}
		return ImmutableList.copyOf(copy);
	}

	public void updateWith(GroupedRankings other) {
		synchronized(rankedItems) {
			for (Entry<Object, List<GroupRankable>> e : other.getRankings().entrySet()) {
				Object group = e.getKey();
				for (GroupRankable r : e.getValue()) {
					updateWith(r, group);
				}
			}
		}
	}

	public void updateWith(GroupRankable r, Object group) {
		synchronized(rankedItems) {
			if (!rankedItems.containsKey(group)) {
				List<GroupRankable> l = Lists.newArrayList();
				rankedItems.put(group, l);
			}

			addOrReplace(r, group);
			rerank(group);
			shrinkRankingsIfNeeded(group);
		}
	}
	
	public void deleteGroup(Object group) {
		synchronized (rankedItems) {
			LOG.info("Deleting group: " + group.toString());
			rankedItems.remove(group);
		}
	}

	private void addOrReplace(GroupRankable r, Object group) {
		Integer rank = findRankOf(r, group);
		if (rank != null) {
			rankedItems.get(group).set(rank, r);
		}
		else {
			rankedItems.get(group).add(r);
		}
	}

	private Integer findRankOf(GroupRankable r, Object group) {
		Object tag = r.getObject();
		for (int rank = 0; rank < rankedItems.get(group).size(); rank++) {
			Object cur = rankedItems.get(group).get(rank).getObject();
			if (cur.equals(tag)) {
				return rank;
			}
		}
		return null;
	}

	private void rerank(Object group) {
		Collections.sort(rankedItems.get(group));
		Collections.reverse(rankedItems.get(group));
	}

	private void shrinkRankingsIfNeeded(Object group) {
		if (rankedItems.get(group).size() > maxSize) {
			rankedItems.get(group).remove(maxSize);
		}
	}

	/**
	 * Removes ranking entries that have a count of zero.
	 */
	 public void pruneZeroCounts() {
		 for (Object group : rankedItems.keySet()) {
			 int i = 0;
			 while (i < rankedItems.get(group).size()) {
				 if (rankedItems.get(group).get(i).getCount() == 0) {
					 rankedItems.get(group).remove(i);
				 }
				 else {
					 i++;
				 }
			 }
		 }
	 }

	 public String toString() {
		 return rankedItems.toString();
	 }

	 /**
	  * Creates a (defensive) copy of itself.
	  */
	 public GroupedRankings copy() {
		 return new GroupedRankings(this);
	 }
}