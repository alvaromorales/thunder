package storm.thunder.util;

import java.io.Serializable;

import com.javadocmd.simplelatlng.LatLng;

public class Fence implements Serializable {
	
	private static final long serialVersionUID = 2316750698571428058L;
	private LatLng point;
	private int radius;
	
	public Fence(double lat, double lon, int radius) {
		this.point = new LatLng(lat, lon);
		this.radius = radius;
	}

	public LatLng getPoint() {
		return point;
	}

	public int getRadius() {
		return radius;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((point == null) ? 0 : point.hashCode());
		result = prime * result + radius;
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
		if (point == null) {
			if (other.point != null)
				return false;
		} else if (!point.equals(other.point))
			return false;
		if (radius != other.radius)
			return false;
		return true;
	}

}
