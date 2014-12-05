package storm.thunder.bolt;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.javadocmd.simplelatlng.LatLng;
import com.javadocmd.simplelatlng.LatLngTool;
import com.javadocmd.simplelatlng.util.LengthUnit;

import storm.thunder.spout.MessagesScheme;
import storm.thunder.spout.TweetScheme;
import storm.thunder.util.Fence;
import storm.thunder.util.TupleHelpers;

import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GeoFilterBolt extends BaseBasicBolt {
	
    private static final Logger LOG = Logger.getLogger(GeoFilterBolt.class);

	public static final String ID_FIELD = "id";
	public static final String TYPE_FIELD = "type";
	public static final String TWEET_FIELD = "tweet";

	private static final long serialVersionUID = 6192361668102197870L;

	private Map<String, String> fenceTypes;
	private Map<String, Fence> fences;

	public GeoFilterBolt() {
		this.fenceTypes = Maps.newHashMap();
		this.fences = Maps.newHashMap();
		updateFences();
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		if (TupleHelpers.isTickTuple(tuple)) {
			updateFences();
		} else {
			String tweet = (String) tuple.getValueByField(TweetScheme.TWEET_FIELD);
			Double lat = (Double) tuple.getValueByField(TweetScheme.LAT_FIELD);
			Double lon = (Double) tuple.getValueByField(TweetScheme.LON_FIELD);

			//Skip tweets that have no location information
			if (lat.equals(TweetScheme.NO_LOCATION) && lon.equals(TweetScheme.NO_LOCATION)) {
				return;
			}

			for (String fence_id : getMatchingFences(new LatLng(lat, lon))) {
				collector.emit(new Values(fence_id, fenceTypes.get(fence_id), tweet));
			}
		}
	}

	private void updateFences() {
		//TODO read redis
		if (fenceTypes.isEmpty()) {
			//generate a dummy fence around NYC
			UUID id = UUID.randomUUID();
			fenceTypes.put(id.toString(), MessagesScheme.TREND_FEATURE);
			
			Fence fence = new Fence(40.7127, 74.0059, 1000);
			fences.put(id.toString(), fence);
		}
		LOG.info("Updated fences");
	}

	private List<String> getMatchingFences(LatLng p) {
		List<String> matchingFences = Lists.newArrayList();

		for (Entry<String, Fence> entry : fences.entrySet()) {
			LatLng fencePoint = entry.getValue().getPoint();
			int radius = entry.getValue().getRadius();
			if (LatLngTool.distance(fencePoint, p, LengthUnit.KILOMETER) <= radius) {
				matchingFences.add(entry.getKey());
			}
		}

		return matchingFences;
	}

	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declare(new Fields(ID_FIELD, TYPE_FIELD, TWEET_FIELD));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		int tickFrequencyInSeconds = 10;
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
		return conf;
	}

}
