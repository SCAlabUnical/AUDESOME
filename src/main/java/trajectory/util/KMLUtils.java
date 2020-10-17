package trajectory.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;

import de.micromata.opengis.kml.v_2_2_0.Boundary;
import de.micromata.opengis.kml.v_2_2_0.Document;
import de.micromata.opengis.kml.v_2_2_0.Feature;
import de.micromata.opengis.kml.v_2_2_0.Folder;
import de.micromata.opengis.kml.v_2_2_0.Kml;
import de.micromata.opengis.kml.v_2_2_0.LinearRing;
import de.micromata.opengis.kml.v_2_2_0.Placemark;


public class KMLUtils {

    public static Map<String, String> lookupFromKml(String path){
        if (path == null || path.equalsIgnoreCase(""))
            throw new IllegalArgumentException();
        File checkPathIntegrity = new File(path);
        if (! checkPathIntegrity.exists())
            throw new IllegalArgumentException("Kml file not found");
        Map<String,String> result = new HashMap<>();
        Kml kmlContent = Kml.unmarshal(checkPathIntegrity);
        try {
            Document document = (Document) kmlContent.getFeature();
            List<Feature> features = document.getFeature();
            LinkedList<Feature> featuresFound = new LinkedList<Feature>();
            for (Feature feature : features) {
                if (feature instanceof Folder) {
                    Folder folder = (Folder) feature;
                    featuresFound.addAll(folder.getFeature());
                }
                else if (feature instanceof Placemark) {
                    featuresFound.add(feature);
                }
                for (Feature f : featuresFound) {
                    Placemark placemark = (Placemark) f;
                    String placesName = placemark.getName().toLowerCase().trim();
                    de.micromata.opengis.kml.v_2_2_0.Polygon poly = (de.micromata.opengis.kml.v_2_2_0.Polygon) placemark
                            .getGeometry();
                    Boundary boundary = poly.getOuterBoundaryIs();
                    LinearRing linear = boundary.getLinearRing();
                    result.put(placesName,GeoUtils.getPolygonAsString(linear.getCoordinates()));
                }
            }
        }catch (Exception e){
            System.out.println("here");
            System.out.println(e);
        }
        return result;
    }


    public static List<Shape> loadShape(String path) {

        List<Shape> shapes = new LinkedList<Shape>();

        Kml obj = Kml.unmarshal(new File(path));

        Document document = (Document) obj.getFeature();
        for (Feature feature : document.getFeature()) {

            Folder folder;
            LinkedList<Feature> features = new LinkedList<>();
            if (feature instanceof Folder) {
                folder = (Folder) feature;
                features.addAll(folder.getFeature());
            }
            if (feature instanceof Placemark) {
                features.add(feature);
            }

            for (Feature f : features) {
                Placemark placemark = (Placemark) f;
                System.out.println(placemark.getName().toLowerCase().trim());
                de.micromata.opengis.kml.v_2_2_0.Polygon poly = (de.micromata.opengis.kml.v_2_2_0.Polygon) placemark
                        .getGeometry();
                Boundary boundary = poly.getOuterBoundaryIs();
                LinearRing linear = boundary.getLinearRing();
                System.out.println(linear.getCoordinates());
                Shape shape = GeoUtils.getPolygonOpenGISCoordinate(true, linear.getCoordinates());
                shapes.add(shape);

            }
        }
        return shapes;

    }

    public static void main(String[] args) {
        String path = "/home/emanuele/Documents/Tesi/SPM/services.fpm/new/04Rome-DSET-Elbow.kml";
        Map<String, String> shapes = KMLUtils.lookupFromKml(path);
        shapes.forEach((x,y)-> System.out.println(x+"-"+GeoUtils.getPolygonFromString(y)));
        System.out.println(shapes.size());
        //        double lon = 41.89032084089485;
//        double lat = 12.492010432057695;
//        Shape point = GeoUtils.getPoint(lat,lon);
//        for (Shape shape : shapes) {
//            System.out.println(GeoUtils.isContained(point,shape));
//        }
        Date d = new Date(1587157683217000L);
        System.out.println(d);

    }

}
