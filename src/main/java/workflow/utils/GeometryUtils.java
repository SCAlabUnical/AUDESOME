package workflow.utils;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.impl.PointImpl;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import de.micromata.opengis.kml.v_2_2_0.Document;
import de.micromata.opengis.kml.v_2_2_0.Kml;
import de.micromata.opengis.kml.v_2_2_0.Feature;
import de.micromata.opengis.kml.v_2_2_0.Folder;
import de.micromata.opengis.kml.v_2_2_0.Placemark;
import de.micromata.opengis.kml.v_2_2_0.Boundary;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.math3.ml.clustering.Cluster;


import java.io.File;
import java.io.IOException;
import java.util.*;

public class GeometryUtils {

	public static Geometry getConvexHull(Point[] points) {
		try {
			Coordinate[] coordinates = new Coordinate[points.length];
			for (int i = 0; i < coordinates.length; i++) {
				coordinates[i] = new Coordinate(points[i].getX(), points[i].getY());
			}
			GeometryFactory gf = new GeometryFactory();
			ConvexHull ch = new ConvexHull(coordinates, gf);
			return ch.getConvexHull();
		}catch (Exception e){
			System.out.println(e);
		}
		return null;
	}

	public static Shape getShape(Geometry g) {
		return new JtsGeometry(g, JtsSpatialContext.GEO, false, false);
	}

	public static String serialize(Geometry geometry, boolean closeFile, Map<String, String> ext) throws IOException {
		if (geometry instanceof Point) {
			return serializePoint((Point) geometry, closeFile, ext);
		} else if (geometry instanceof LineString) {
			// return serializeLineString((LineString) geometry);
		} else if (geometry instanceof Polygon) {
			return serializePolygon((Polygon) geometry, closeFile, ext);
		} else if (geometry instanceof MultiPolygon) {
			System.err.println("Multipolygon "+ext.get("name"));
		}
		else {
			throw new IllegalArgumentException("Geometry type [" + geometry.getGeometryType() + "] not supported");
		}
		return null;
	}

	public static Geometry getPolygon(Point... points) {

		LinkedList<Coordinate> coordinates = new LinkedList<Coordinate>();
		for (int i = 0; i < points.length; i++) {
			coordinates.add(new Coordinate(points[i].getX(), points[i].getY()));
		}
		coordinates.add(coordinates.get(0));
		// Collections.sort(coordinates, new workflow.utils.ClockwiseCoordinateComparator());
		GeometryFactory gf = new GeometryFactory();

		LinearRing linear = gf.createLinearRing(coordinates.toArray(new Coordinate[0]));
		Polygon poly = new Polygon(linear, null, gf);
		return poly;
	}

	private static String serializePoint(Point geometry, boolean closeFile, Map<String, String> extendedData) {
		StringBuilder sb = new StringBuilder();
		if (closeFile) {
			sb.append(
					"<?xml version=\"1.0\" encoding=\"UTF-8\"?><kml xmlns=\"http://www.opengis.net/kml/2.2\"><Document>");
		}
		HashMap<String, String> ext = generateExtendedDataString(extendedData);
		sb.append("<Placemark>" + ext.get("data") + "<Point><coordinates>" + geometry.getX() + "," + geometry.getY()
				+ "</coordinates></Point></Placemark>");
		if (closeFile) {
			sb.append("</Document></kml>");
		}
		return sb.toString();
	}

	private static String serializePolygon(Polygon geometry, boolean closeFile, Map<String, String> extendedData) {

		StringBuilder sb = new StringBuilder();

		if (closeFile) {
			sb.append(
					"<?xml version=\"1.0\" encoding=\"UTF-8\"?><kml xmlns=\"http://www.opengis.net/kml/2.2\"><Document>");
		}

		HashMap<String, String> ext = generateExtendedDataString(extendedData);
		sb.append("<Placemark>" + ext.get("data")
				+ "<Polygon><outerBoundaryIs><LinearRing><tessellate>0</tessellate><coordinates>");
		Coordinate[] coordinates = geometry.getCoordinates();
		List<Coordinate> pointsList = new LinkedList<Coordinate>();

		double sumLat = 0;
		double sumLng = 0;
		for (Coordinate coordinate : coordinates) {
			pointsList.add(coordinate);
			sumLat += coordinate.y;
			sumLng += coordinate.x;
		}
		Coordinate reference = new Coordinate(sumLng / coordinates.length, sumLat / coordinates.length);
		Collections.sort(pointsList, new ClockwiseCoordinateComparator(reference));
		Coordinate tmp = pointsList.remove(coordinates.length - 1);
		pointsList.add(0, tmp);
		for (Coordinate c : pointsList) {
			sb.append(c.x + "," + c.y + ",0.0 ");
		}
		sb.append("</coordinates></LinearRing></outerBoundaryIs></Polygon></Placemark>");

		if (ext.containsKey("style"))
			sb.append(ext.get("style"));

		if (closeFile) {
			sb.append("</Document></kml>");
		}

		return sb.toString();
	}

	private static HashMap<String, String> generateExtendedDataString(Map<String, String> extendedData) {
		String ext = "";
		String preText = "";
		HashMap<String, String> ret = new HashMap<String, String>();
		if (extendedData != null && extendedData.size() > 0) {
			ext = "<ExtendedData>";
			for (Map.Entry<String, String> entry : extendedData.entrySet()) {
				if (entry.getKey().equals("styleUrl") || entry.getKey().equals("description")) {
					preText += "<styleUrl>" + entry.getValue().trim() + "</styleUrl>";
				} else if (entry.getKey().equals("color")) {
					preText += "<styleUrl>#poly-" + entry.getValue().trim() + "</styleUrl>";
					String style = "<Style id=\"poly-" + entry.getValue().trim() + "\">" + "<LineStyle>" + "<color>"
							+ entry.getValue().trim() + "</color>" + "	<width>2</width>" + "</LineStyle>"
							+ "<PolyStyle>" + "<color>" + entry.getValue().trim() + "</color>" + "	<fill>1</fill>"
							+ "<outline>1</outline>" + "</PolyStyle></Style>";
					ret.put("style", style);

				} else if (entry.getKey().equals("description")) {
					preText += "<description><![CDATA[descrizione:" + entry.getValue() + "]]></description>";
				} else if (entry.getKey().equals("name")) {
					preText += "<name>" + entry.getValue() + "</name>";
				} else {
					ext += "<Data name=\"" + entry.getKey() + "\"><value>" + entry.getValue() + "</value></Data>";
				}
			}
			ext += "</ExtendedData>";
		}
		ret.put("data", preText + ext);
		return ret;
	}

	public static List<Geometry> loadShape(String path) {

		List<Geometry> shapes = new LinkedList<Geometry>();

		Kml obj = Kml.unmarshal(new File(path));

		Document document = (Document) obj.getFeature();
		for (Feature feature : document.getFeature()) {

			Folder folder;
			LinkedList<Feature> features = new LinkedList<Feature>();
			if (feature instanceof Folder) {
				folder = (Folder) feature;
				features.addAll(folder.getFeature());
			}
			if (feature instanceof Placemark) {
				features.add(feature);
			}

			for (Feature f : features) {
				Placemark placemark = (Placemark) f;

				de.micromata.opengis.kml.v_2_2_0.Polygon poly = (de.micromata.opengis.kml.v_2_2_0.Polygon) placemark
						.getGeometry();
				Boundary boundary = poly.getOuterBoundaryIs();
				de.micromata.opengis.kml.v_2_2_0.LinearRing linear = boundary.getLinearRing();
				Shape shape = GeoUtils.getPolygonOpenGISCoordinate(true, linear.getCoordinates());
				shapes.add(((JtsGeometry) shape).getGeom());

			}
		}
		return shapes;

	}

	public static Map<String, Geometry> loadShapeMap(String path) {

		Map<String, Geometry> shapes = new HashMap<String, Geometry>();

		Kml obj = Kml.unmarshal(new File(path));

		Document document = (Document) obj.getFeature();
		for (Feature feature : document.getFeature()) {

			Folder folder;
			LinkedList<Feature> features = new LinkedList<Feature>();
			if (feature instanceof Folder) {
				folder = (Folder) feature;
				features.addAll(folder.getFeature());
			}
			if (feature instanceof Placemark) {
				features.add(feature);
			}

			for (Feature f : features) {
				Placemark placemark = (Placemark) f;
				de.micromata.opengis.kml.v_2_2_0.Polygon poly = (de.micromata.opengis.kml.v_2_2_0.Polygon) placemark
						.getGeometry();
				Boundary boundary = poly.getOuterBoundaryIs();
				de.micromata.opengis.kml.v_2_2_0.LinearRing linear = boundary.getLinearRing();
				Shape shape = GeoUtils.getPolygonOpenGISCoordinate(true, linear.getCoordinates());
				shapes.put(placemark.getName().toLowerCase().trim(), ((JtsGeometry) shape).getGeom());

			}
		}
		return shapes;

	}

	public static void main(String[] args) {
		GeometryFactory g = new GeometryFactory();
	}

	public static Geometry getRoI(Iterable<ClusterPoint> ppoints, double eps, int numNeighbours) {
		DBSCANRoI<ClusterPoint> dbscan = new DBSCANRoI<>(eps, numNeighbours);
		try {
			List<Cluster<ClusterPoint>> clusters = dbscan.cluster(IterableUtils.toList(ppoints));
			Cluster<ClusterPoint> cMax = findMaxCluster(clusters, numNeighbours);
			if (cMax != null) {
				Point[] points = cMax.getPoints().stream().map(x -> new PointImpl(x.getX(), x.getY(), SpatialContext.GEO)).toArray(PointImpl[]::new);
				if(points.length>3) {
					Geometry geoMax = GeometryUtils.getConvexHull(points);
					if((geoMax instanceof Polygon)) {
						return geoMax;
					}
				}
			}
		}catch (Exception e) {
			System.err.println(e);
		}

		return null;
	}

	private static Cluster<ClusterPoint> findMaxCluster(List<Cluster<ClusterPoint>> clusters, int minPts) {
		int max = 0;
		Cluster<ClusterPoint> cMax = null;
		for (Cluster<ClusterPoint> cluster : clusters) {
			if (cluster.getPoints().size() > minPts && cluster.getPoints().size() > max) {
				max = cluster.getPoints().size();
				cMax = cluster;
			}
		}
		return cMax;
	}

}
