package workflow.utils;

import java.io.Serializable;

public class Cell implements Serializable{
	private double lat, lon;
	
	public Cell(double lon, double lat) {
		this.lat = lat;
		this.lon = lon;		
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(lat);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(lon);
		result = prime * result + (int) (temp ^ (temp >>> 32));
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
		Cell other = (Cell) obj;
		if (Double.doubleToLongBits(lat) != Double.doubleToLongBits(other.lat))
			return false;
		if (Double.doubleToLongBits(lon) != Double.doubleToLongBits(other.lon))
			return false;
		return true;
	}




	public double getLat() {
		return lat;
	}
	public double getLon() {
		return lon;
	}
	@Override
	public String toString() {
		return "workflow.utils.Cell [lat=" + lat + ", lon=" + lon + "]";
	}
	
}
