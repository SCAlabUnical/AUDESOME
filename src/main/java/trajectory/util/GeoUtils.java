package trajectory.util;

import java.awt.Color;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Random;

import org.apache.commons.math3.exception.DimensionMismatchException;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.distance.DistanceCalculator;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.distance.GeodesicSphereDistCalc;
import com.spatial4j.core.exception.InvalidShapeException;
import com.spatial4j.core.shape.Circle;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class GeoUtils {

	final static double RAD = DistanceUtils.EARTH_MEAN_RADIUS_KM;
	final static SpatialContext ctx = SpatialContext.GEO;
	final static SpatialContext jtsctx = JtsSpatialContext.GEO;
	final static double SURFACE_AREA_ONE_SQUARE_DEGREE = 12365.1613;
	final static double STEP1M = 8.992909382672273E-6;
	final static DistanceCalculator haversine = new GeodesicSphereDistCalc.Haversine();


	public static double getDistanceMeters(Point a, Point b) {
		double dist_degree = haversine.distance(a, b);
		double dist_rad = DistanceUtils.toRadians(dist_degree);
		return dist_rad * RAD * 1000;
	}

	public static double getAreaSquaredKm(Shape p) {

		return p.getArea(ctx) * SURFACE_AREA_ONE_SQUARE_DEGREE;
	}

	public static double distance(double[] a, double[] b) throws DimensionMismatchException {
		double dist_degree = haversine.distance((Point) getPoint(a[0], a[1]), (Point) getPoint(b[0], b[1]));
		double dist_rad = DistanceUtils.toRadians(dist_degree);
		return dist_rad * RAD * 1000;
	}

	public static double distance(Point a, Point b) {
		double dist_degree = haversine.distance(a, b);
		double dist_rad = DistanceUtils.toRadians(dist_degree);
		return dist_rad * RAD * 1000;
	}

	public static double getIntersectArea(Shape a, Shape b) {
		Geometry g = ((JtsGeometry) a).getGeom();
		Geometry r = ((JtsGeometry) b).getGeom();
		return g.intersection(r).getArea();
	}

	public static double getDistanceKM(Point a, Point b) {
		DistanceCalculator haversine = new GeodesicSphereDistCalc.Haversine();
		double dist_degree = haversine.distance(a, b);
		double dist_rad = DistanceUtils.toRadians(dist_degree);
		return dist_rad * RAD;

	}

	public static double degreeToKM(double degrees) {
		double dist_rad = DistanceUtils.toRadians(degrees);
		return dist_rad * RAD;
	}

	public static double degreeToMeters(double degrees) {
		double dist_rad = DistanceUtils.toRadians(degrees);
		return dist_rad * RAD * 1000;
	}

	public static double metersToDegree(double meters) {
		double dist_rad = meters / (RAD * 1000);
		return DistanceUtils.toDegrees(dist_rad);
	}

	public static Circle getCircle(Point p, double radiusInMeters) {
		return ctx.makeCircle(p, GeoUtils.metersToDegree(radiusInMeters));
	}

	public static Rectangle getRectangle(Point lowerLeft, Point upperRight) {
		return ctx.makeRectangle(lowerLeft, upperRight);
	}

	public static Shape getLineString(Point... points) {
		String pointString = "";
		for (Point point : points) {
			pointString += "," + point.getY() + " " + point.getX();
		}
		return jtsctx.readShape("LINESTRING (" + pointString.substring(1) + ")");
	}

	public static Shape getLineString(String pointString) {
		return jtsctx.readShape("LINESTRING (" + pointString + ")");
	}

	public static Shape getPolygon(Point... points)
			throws InvalidShapeException, IOException, ParseException {
		String tmp = "[[";
		for (Point point : points) {
			tmp += "[" + point.getX() + "," + point.getY() + "],";
		}
		tmp = tmp.substring(0, tmp.length() - 1);
		tmp += "]]";
		String shapeString = "{'type':'Polygon','coordinates':" + tmp + "}";

		return jtsctx.readShape(shapeString);
	}


	public static Shape getPolygon(boolean close, Point... points)
			throws InvalidShapeException, IOException, ParseException {
		String tmp = "[[";
		for (Point point : points) {
			tmp += "[" + point.getX() + "," + point.getY() + "],";
		}
		if (close) {
			tmp += "[" + points[0].getX() + "," + points[0].getY() + "],";
		}
		tmp = tmp.substring(0, tmp.length() - 1);
		tmp += "]]";
		String shapeString = "{'type':'Polygon','coordinates':" + tmp + "}";

		return jtsctx.readShape(shapeString);
	}

	public static Shape getPolygon(boolean close, Coordinate... points)
			throws InvalidShapeException, IOException, ParseException {
		String tmp = "[[";
		for (Coordinate point : points) {
			tmp += "[" + point.x + "," + point.y + "],";
		}
		if (close) {
			tmp += "[" + points[0].x + "," + points[0].y + "],";
		}
		tmp = tmp.substring(0, tmp.length() - 1);
		tmp += "]]";
		String shapeString = "{'type':'Polygon','coordinates':" + tmp + "}";

		return jtsctx.readShape(shapeString);
	}

	public static Shape getPolygon(boolean close, de.micromata.opengis.kml.v_2_2_0.Coordinate... points)
			throws InvalidShapeException, IOException, ParseException {
		String tmp = "[[";
		for (de.micromata.opengis.kml.v_2_2_0.Coordinate point : points) {
			tmp += "[" + point.getLongitude() + "," + point.getLatitude() + "],";
		}
		if (close) {
			tmp += "[" + points[0].getLongitude() + "," + points[0].getLatitude() + "],";
		}
		tmp = tmp.substring(0, tmp.length() - 1);
		tmp += "]]";
		String shapeString = "{'type':'Polygon','coordinates':" + tmp + "}";

		return jtsctx.readShape(shapeString);
	}


	public static Shape getPolygon(boolean close, Collection<Point> points) {
		String tmp = "[[";
		Point pFirst = null;
		for (Point point : points) {
			if (pFirst == null) pFirst = point;
			tmp += "[" + point.getX() + "," + point.getY() + "],";
		}
		if (close) {
			tmp += "[" + pFirst.getX() + "," + pFirst.getY() + "],";
		}
		tmp = tmp.substring(0, tmp.length() - 1);
		tmp += "]]";
		String shapeString = "{'type':'Polygon','coordinates':" + tmp + "}";
		try {
			return jtsctx.readShape(shapeString);
		} catch (Exception e) {
			return null;
		}
	}

	public static Shape getConvexHull(Shape s) {
		JtsGeometry gTmp = (JtsGeometry) s;
		Geometry geoHull = gTmp.getGeom().convexHull();
		return new JtsGeometry(geoHull, JtsSpatialContext.GEO, false, false);
	}

	public static Shape getPolygonOpenGISCoordinate(boolean close, Collection<de.micromata.opengis.kml.v_2_2_0.Coordinate> points) {
		String tmp = "";
		de.micromata.opengis.kml.v_2_2_0.Coordinate pFirst = null;
		for (de.micromata.opengis.kml.v_2_2_0.Coordinate point : points) {
			if (pFirst == null) pFirst = point;
			tmp +=  ","+point.getLongitude() + " " + point.getLatitude();
		}
		tmp += ")";
		String shapeString = "POLYGON(("+tmp.substring(1)+")";
		try {
			return jtsctx.getFormats().getWktReader().read(shapeString);
		} catch (Exception e) {
			System.out.println(e);
			return null;
		}
	}


	public static String getPolygonAsString(Collection<de.micromata.opengis.kml.v_2_2_0.Coordinate> points){
		String tmp = "";
		de.micromata.opengis.kml.v_2_2_0.Coordinate pFirst = null;
		de.micromata.opengis.kml.v_2_2_0.Coordinate pLast = null;
		for (de.micromata.opengis.kml.v_2_2_0.Coordinate point : points) {
			if (pFirst == null) pFirst = point;
			tmp +=  ","+point.getLongitude() + " " + point.getLatitude();
			pLast = point;
		}
		if (pFirst!=null && !pFirst.equals(pLast)){
			tmp +=  ","+pFirst.getLongitude() + " " + pFirst.getLatitude();
		}
		tmp += ")";
		String shapeString = "POLYGON(("+tmp.substring(1)+")";
		return shapeString;
	}

	public static Shape getPolygonFromString(String shapeString){
		try {
			return jtsctx.getFormats().getWktReader().read(shapeString);
		} catch (Exception e) {
			System.out.println(e);
		}

		return null;
	}


	public static Shape getPolygon(String polygon) throws InvalidShapeException, IOException, ParseException {
		return jtsctx.readShape(polygon);
	}

	public static boolean isContained(Shape inner, Shape container) {
		if (container.equals(inner))
			return true;
		if (container.relate(inner) == SpatialRelation.CONTAINS)
			return true;
		return false;
	}

	public static boolean isWithin(Shape inner, Shape container) {
		if (container.equals(inner))
			return true;
		if (container.relate(inner) == SpatialRelation.WITHIN)
			return true;
		return false;
	}

	public static SpatialContext getSpatialContext() {
		return ctx;
	}

	public static Shape getPoint(String pointString) {
		String[] values = pointString.split(" ");
		return ctx.makePoint(Double.parseDouble(values[1]), Double.parseDouble(values[0]));
	}

	public static Shape getPoint(double longitude, double latitude) {
		return ctx.makePoint(longitude, latitude);
	}

	public static Color randomColor() {
		Random random = new Random(); // Probably really put this somewhere
		// where it gets executed only once
		int red = random.nextInt(256);
		int green = random.nextInt(256);
		int blue = random.nextInt(256);
		return new Color(red, green, blue, 120);
	}

}