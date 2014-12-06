package storm.thunder.tools;

import java.io.Serializable;

import storm.thunder.spout.MessagesScheme;

public class HashtagFence implements Serializable {
	
	private static final long serialVersionUID = -5758175722450043199L;
	private String fenceId;
	private String type = MessagesScheme.TREND_FEATURE;
	private String hashtag;
	
	public HashtagFence(String fenceId, String hashtag) {
		this.fenceId = fenceId;
		this.hashtag = hashtag;
	}

	public String getFenceId() {
		return fenceId;
	}

	public String getType() {
		return type;
	}

	public String getHashtag() {
		return hashtag;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fenceId == null) ? 0 : fenceId.hashCode());
		result = prime * result + ((hashtag == null) ? 0 : hashtag.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HashtagFence other = (HashtagFence) obj;
		if (fenceId == null) {
			if (other.fenceId != null)
				return false;
		} else if (!fenceId.equals(other.fenceId))
			return false;
		if (hashtag == null) {
			if (other.hashtag != null)
				return false;
		} else if (!hashtag.equals(other.hashtag))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return hashtag;
	}

}
