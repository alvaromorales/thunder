package storm.thunder.spout;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

public class TweetScheme implements Scheme {

	//Tweet field names
	public static final String TWEET_ID_FIELD = "tweet_id";
	public static final String TIME_FIELD = "time";
	public static final String LAT_FIELD = "lat";
	public static final String LON_FIELD = "lon";
	public static final String GOOG_X_FIELD = "goog_x";
	public static final String GOOG_Y_FIELD = "goog_y";
	public static final String SENDER_ID_FIELD = "sender_id";
	public static final String SENDER_NAME_FIELD = "sender_name";
	public static final String SOURCE_FIELD = "source";
	public static final String REPLY_TO_USER_ID_FIELD = "reply_to_user_id";
	public static final String REPLY_TO_TWEET_ID_FIELD = "reply_to_tweet_id";
	public static final String PLACE_ID_FIELD = "place_id";
	public static final String TWEET_FIELD = "tweet";
	
	//Tweet field indices
	public static final int TWEET_ID_COL = 0;
	public static final int TIME_COL = 1;
	public static final int LAT_COL = 2;
	public static final int LON_COL = 3;
	public static final int GOOG_X_COL = 4;
	public static final int GOOG_Y_COL = 5;
	public static final int SENDER_ID_COL = 6;
	public static final int SENDER_NAME_COL = 7;
	public static final int SOURCE_COL = 8;
	public static final int REPLY_TO_USER_ID_COL = 9;
	public static final int REPLY_TO_TWEET_ID_COL = 10;
	public static final int PLACE_ID_COL = 11;
	public static final int TWEET_COL = 12;
	
	public static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
	public static final Double NO_LOCATION = null;

	private static final long serialVersionUID = 4162296434312733491L;

	public List<Object> deserialize(byte[] ser) {
		List<Object> tvals = Lists.newArrayList();
		try {
			String tweet = new String(ser, "UTF-8");
			String[] fields = tweet.split("\t");
			
			tvals.add(fields[TWEET_ID_COL]);
			
			DateTimeFormatter formatter = DateTimeFormat.forPattern(DATETIME_PATTERN);
			DateTime time = formatter.parseDateTime(fields[TIME_COL]);
			tvals.add(time);
			
			Double lat = parseLocation(fields[LAT_COL]);
			tvals.add(lat);

			Double lon = parseLocation(fields[LON_COL]);
			tvals.add(lon);

			Double goog_x = parseLocation(fields[GOOG_X_COL]);
			tvals.add(goog_x);

			Double goog_y = parseLocation(fields[GOOG_Y_COL]);
			tvals.add(goog_y);
			
			tvals.add(fields[SENDER_ID_COL]);
			tvals.add(fields[SENDER_NAME_COL]);
			tvals.add(fields[SOURCE_COL]);
			tvals.add(fields[REPLY_TO_USER_ID_COL]);
			tvals.add(fields[REPLY_TO_TWEET_ID_COL]);
			tvals.add(fields[PLACE_ID_COL]);
			tvals.add(fields[TWEET_COL]);

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return tvals;
	}
	
	private Double parseLocation(String location) {
		Double loc;
		if (!Strings.isNullOrEmpty(location)) {
			loc = Double.valueOf(location);
		} else {
			loc = NO_LOCATION;
		}	
		return loc;
	}

	public Fields getOutputFields() {
		return new Fields(TWEET_ID_FIELD, TIME_FIELD, LAT_FIELD, LON_FIELD, GOOG_X_FIELD,
				GOOG_Y_FIELD, SENDER_ID_FIELD, SENDER_NAME_FIELD, SOURCE_FIELD,
				REPLY_TO_USER_ID_FIELD, REPLY_TO_TWEET_ID_FIELD,
				PLACE_ID_FIELD, TWEET_FIELD);
	}

}
