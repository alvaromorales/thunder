package storm.thunder.bolt;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import redis.clients.jedis.Jedis;
import storm.thunder.tools.Fence;
import storm.thunder.tools.FencesDB;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;

public abstract class AbstractFenceBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1550490462033094045L;
    private static final Logger LOG = Logger.getLogger(AbstractFenceBolt.class);
	
	private Map<String, Fence> fences;
	public OutputCollector collector;
	private Jedis jedis;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.jedis = new Jedis(FencesDB.REDIS_HOST, FencesDB.REDIS_PORT);
		this.fences = Maps.newHashMap();
		updateFences();
	}
	
	protected void updateFences() {
		synchronized(fences) {
			Map<String, Fence> newFences = Maps.newHashMap();

			Set<String> fenceIds = jedis.smembers(FencesDB.FENCE_INDEX_KEY);
			for (String id : fenceIds) {
				String fenceStr = jedis.get(id);
				Fence f = Fence.from(id, fenceStr);

				newFences.put(id, f);
			}
			this.fences = newFences;
		}
		LOG.info("Updated fences. Fences: " + fences.keySet());
	}
	
	public Collection<Fence> getFences() {
		return fences.values();
	}

	public Fence getFence(String id) {
		return fences.get(id);
	}

	public String getFenceType(String id) {
		Fence fence = fences.get(id);
		if (fence != null) {
			return fence.getType();
		}
		return null;
	}

}
