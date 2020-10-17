package workflow.utils;

import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.ml.distance.DistanceMeasure;


public class LngLatDistance implements DistanceMeasure {

	/**
	 * Restituisce la distanza in metri tra due punti geografici
	 * 
	 * @see DistanceMeasure#compute(double[], double[])
	 */
	@Override
	public double compute(double[] a, double[] b) throws DimensionMismatchException {
		return GeoUtils.distance(a,b);
	}

}
