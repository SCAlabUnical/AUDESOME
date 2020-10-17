package workflow.utils;

import com.spatial4j.core.shape.Point;

import java.util.Comparator;

public class ClockwisePointComparator implements Comparator<Point> {
	
	private Point reference;

	public ClockwisePointComparator(Point reference) {
		this.reference =reference;
	}
	
	@Override
	public int compare(Point a, Point b) {
			// Variables to Store the atans
			double aTanA, aTanB;

			// Fetch the atans
			aTanA = Math.atan2(a.getY() - reference.getY(), a.getX() - reference.getX());
			aTanB = Math.atan2(b.getY() - reference.getY(), b.getX() - reference.getX());

			// Determine next point in Clockwise rotation
			if (aTanA < aTanB)
				return -1;
			else if (aTanB < aTanA)
				return 1;
			return 0;

	}

}
