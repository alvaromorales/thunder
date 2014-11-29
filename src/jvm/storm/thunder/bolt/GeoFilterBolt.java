package storm.thunder.bolt;

import java.util.Random;

import storm.thunder.tools.CountryTopic;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GeoFilterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 6192361668102197870L;

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String topic = (String) tuple.getValueByField("topic");
		Double lat = (Double) tuple.getValueByField("lat");
		Double lon = (Double) tuple.getValueByField("lon");
		
		String country = getCountry(lat, lon);
		
		collector.emit(new Values(new CountryTopic(country, topic)));
	}
	
	private String getCountry(Double lat, Double lon) {
		//TODO lookup country in places DB
		Random r = new Random();
		if (r.nextDouble() < 0.6) {
			return "USA";
		} else {
			return "Spain";
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declare(new Fields("countryTopic"));
	}

}
