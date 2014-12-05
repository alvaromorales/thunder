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

}
