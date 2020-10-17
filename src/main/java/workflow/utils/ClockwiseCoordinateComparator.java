package workflow.utils;

import com.vividsolutions.jts.geom.Coordinate;

import java.util.Comparator;

public class ClockwiseCoordinateComparator implements Comparator<Coordinate> {

	
	private Coordinate reference;

	public ClockwiseCoordinateComparator(Coordinate reference) {
		this.reference =reference;
	}
	

	@Override
	public int compare(Coordinate a, Coordinate b) {
			// Variables to Store the atans
			double aTanA, aTanB;	
			

			

			// Fetch the atans
			aTanA = Math.atan2(a.y - reference.y, a.x - reference.x);
			aTanB = Math.atan2(b.y - reference.y, b.x - reference.x);

			// Determine next point in Clockwise rotation
			if (aTanA < aTanB)
				return -1;
			else if (aTanB < aTanA)
				return 1;
			return 0;

	}

}
