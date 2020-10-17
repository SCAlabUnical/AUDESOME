package workflow.utils;

import org.apache.commons.math3.ml.clustering.Clusterable;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.impl.PointImpl;
import java.io.Serializable;

public class ClusterPoint extends PointImpl implements Clusterable, Serializable  {
	double x, y;

	public ClusterPoint(double x, double y, SpatialContext ctx) {
		super(x, y, ctx);
		this.x = x;
		this.y = y;
	}

	@Override
	public double[] getPoint() {
		double[] p = {this.getX(), this.getY()};
		return p;
	}

	@Override
	public boolean equals(Object o) {
		ClusterPoint p = (ClusterPoint)o;
		if (p.getX() == this.getX() && p.getY() == this.getY())
			return true;
		return false;
	}

	public double getX() {
		return x;
	}

	public double getY() {
		return y;
	}
	
	
	

}
