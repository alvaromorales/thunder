package storm.thunder.bolt;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.javadocmd.simplelatlng.LatLng;
import com.javadocmd.simplelatlng.LatLngTool;
import com.javadocmd.simplelatlng.util.LengthUnit;

import storm.thunder.spout.MessagesScheme;
import storm.thunder.spout.TweetScheme;
import storm.thunder.tools.Count;
import storm.thunder.tools.Fence;
import storm.thunder.util.TopologyFields;
import storm.thunder.util.TupleHelpers;

import backtype.storm.Config;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GeoFilterBolt extends AbstractFenceBolt {
	
    private static final Logger LOG = Logger.getLogger(GeoFilterBolt.class);

	private static final long serialVersionUID = 6192361668102197870L;
	
	@Override
	public void execute(Tuple tuple) {
		if (TupleHelpers.isTickTuple(tuple)) {
			updateFences();
		} else {
			String tweet = (String) tuple.getValueByField(TweetScheme.TWEET_FIELD);
			Double lat = (Double) tuple.getValueByField(TweetScheme.LAT_FIELD);
			Double lon = (Double) tuple.getValueByField(TweetScheme.LON_FIELD);

			collector.emit(MessagesScheme.TOTAL_FEATURE, new Values(new Count(MessagesScheme.TOTAL_COUNT_FIELD, 1)));

			//Skip tweets that have no location information
			if (lat.equals(TweetScheme.NO_LOCATION) && lon.equals(TweetScheme.NO_LOCATION)) {
				return;
			}
			
			List<String> matchingFences = getMatchingFences(new LatLng(lat, lon));
			LOG.debug("Tweet matched the following " + matchingFences.size() + " fence(s):" + matchingFences);

			for (String fenceId : matchingFences) {
				String type = getFenceType(fenceId);
				if (type.equals(MessagesScheme.TREND_FEATURE)) {
					LOG.debug("Emitting to trend branch: " + fenceId);
					collector.emit(MessagesScheme.TREND_FEATURE, new Values(fenceId, type, tweet));
				} else if (type.equals(MessagesScheme.COUNT_FEATURE)) {
					LOG.debug("Emitting to count branch: " + fenceId);
					collector.emit(MessagesScheme.COUNT_FEATURE, new Values(new Count(fenceId, 1)));
				}
			}
		}
	}

	private List<String> getMatchingFences(LatLng p) {
		List<String> matchingFences = Lists.newArrayList();

		for (Fence fence : getFences()) {
			LatLng fencePoint = fence.getPoint();
			int radius = fence.getRadius();
			if (LatLngTool.distance(fencePoint, p, LengthUnit.KILOMETER) <= radius) {
				matchingFences.add(fence.getId());
			}
		}

		return matchingFences;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(MessagesScheme.TREND_FEATURE, new Fields(TopologyFields.ID_FIELD, TopologyFields.TYPE_FIELD, TopologyFields.TWEET_FIELD));
		declarer.declareStream(MessagesScheme.COUNT_FEATURE, new Fields(TopologyFields.COUNT_FIELD));
		declarer.declareStream(MessagesScheme.TOTAL_FEATURE, new Fields(TopologyFields.COUNT_FIELD));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		int tickFrequencyInSeconds = 10;
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
		return conf;
	}

	@Override
	public void cleanupFences() {
		// do nothing, cleanup handled in update
	}

}
