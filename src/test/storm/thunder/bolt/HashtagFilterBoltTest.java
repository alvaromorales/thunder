package storm.thunder.bolt;

import static org.junit.Assert.*;

import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Sets;

public class HashtagFilterBoltTest {

	@Test
	public void testHashtags() {
		String tweet = "Yessss..olive garden in my belly #happy #full #nomnom #foodbaby";
		Set<String> expected = Sets.newHashSet("#happy", "#full", "#nomnom", "#foodbaby");
		assertEquals("Wrong hashtags", expected, HashtagFilterBolt.getHashtags(tweet));
	}
	
	public void testHashtagsPunctuation() {
		String tweet = "\"@NYRangers: Hey Kendrick Lamar...the #KingOfNY is #HLundqvist30. #NYR #NYRap\" straight up";
		Set<String> expected = Sets.newHashSet("#kingofny", "#nyr", "#nyrap", "hlundqvist30");
		assertEquals("Wrong hashtags", expected, HashtagFilterBolt.getHashtags(tweet));
	}
}
