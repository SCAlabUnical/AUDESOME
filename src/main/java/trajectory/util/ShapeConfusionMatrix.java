package trajectory.util;

import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Geometry;

public class ShapeConfusionMatrix {
	
	Geometry real;
	Geometry found;
	

	public ShapeConfusionMatrix(Shape real, Shape found) {
		super();
		this.real = ((JtsGeometry) real).getGeom();
		this.found = ((JtsGeometry) found).getGeom();
	}
	
	public ShapeConfusionMatrix(Geometry real, Geometry found) {
		super();
		this.real = real;
		this.found = found;
	}

	public double TP() {
		return found.intersection(real).getArea();
	}

	public double FP() {
		return found.difference(real).getArea();
	}

	public double FN() {
		return real.difference(found).getArea();
	}

	public double getPrecisionP() {
		double tp = TP();
		double fp = FP();
		return tp / (tp + fp);
	}

	public double getRecallP() {
		double tp = TP();
		double fn = FN();
		return tp / (tp + fn);
	}

	public double getF1P() {
		double recallP = getRecallP();
		double precisionP = getPrecisionP();
		return 2.0 * recallP * precisionP / (recallP + precisionP);
	}

}
