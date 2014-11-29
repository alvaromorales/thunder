package storm.thunder.tools;

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class TopicMap {
	
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
	
	public Collection<String> getTopics() {
		return topics;
	}
	
	public Collection<String> getMatchingTopics(String word) {
		return topicWords.get(word);
	}

}
