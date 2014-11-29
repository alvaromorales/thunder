package storm.thunder.tools;

public class CountryTopic {
	
	private final String country;
	private final String topic;
	
	public CountryTopic(String country, String topic) {
		this.country = country;
		this.topic = topic;
	}

	public String getCountry() {
		return country;
	}

	public String getTopic() {
		return topic;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((country == null) ? 0 : country.hashCode());
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
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
		CountryTopic other = (CountryTopic) obj;
		if (country == null) {
			if (other.country != null)
				return false;
		} else if (!country.equals(other.country))
			return false;
		if (topic == null) {
			if (other.topic != null)
				return false;
		} else if (!topic.equals(other.topic))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "CountryTopic [country=" + country + ", topic=" + topic + "]";
	}

}
