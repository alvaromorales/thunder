package storm.thunder.tools;

import java.io.Serializable;

import com.javadocmd.simplelatlng.LatLng;

public class Fence implements Serializable {
	
	private static final long serialVersionUID = 2316750698571428058L;
	private String id;
	private LatLng point;
	private int radius;
	private String type;
	
	public Fence(String id, double lat, double lon, int radius, String type) {
		this.id = id;
		this.point = new LatLng(lat, lon);
		this.radius = radius;
		this.type = type;
	}
	
	public static Fence from(String id, String fenceStr) {
		String[] fields = fenceStr.split("\\|");
		if (fields.length != 4) {
			throw new IllegalArgumentException("Not a valid fenceString: " + fenceStr);
		}
		
		try {
			double lat = Double.valueOf(fields[FencesDB.LAT_COL]);
			double lon = Double.valueOf(fields[FencesDB.LON_COL]);
			int radius = Integer.valueOf(fields[FencesDB.RADIUS_COL]);
			String type = fields[FencesDB.TYPE_COL];
			return new Fence(id, lat, lon, radius, type);
		} catch(ClassCastException e) {
			throw new IllegalArgumentException("Not a valid fenceString: " + fenceStr);
		}
	}
	
	public String getId() {
		return id;
	}

	public LatLng getPoint() {
		return point;
	}

	public int getRadius() {
		return radius;
	}
	
	public String getType() {
		return type;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
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
		Fence other = (Fence) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

}
