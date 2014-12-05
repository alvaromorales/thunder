package storm.thunder.tools;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class TopicMap implements Serializable {
	
	private static final long serialVersionUID = 9044367388617908538L;
	private Multimap<String, String> topicWords;
	private Set<String> topics;
	
	public TopicMap() {
		this.topicWords = HashMultimap.create();
		this.topics = Sets.newHashSet();
	}
	
	public void addTopic(String name, Set<String> words) {
		this.topics.add(name);
		for (String word : words) {
			topicWords.put(word, name);
		}
	}
	
	@Override
	public String toString() {
		return "TopicMap [topicWords=" + topicWords + ", topics=" + topics
				+ "]";
	}

	public Collection<String> getTopics() {
		return topics;
	}
	
	public Collection<String> getMatchingTopics(String word) {
		return topicWords.get(word);
	}

}
