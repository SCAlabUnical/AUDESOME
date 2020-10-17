package distance;

import com.google.common.collect.Ordering;
import com.google.common.math.Quantiles;
import com.google.common.math.Stats;
import com.spatial4j.core.shape.Point;
import trajectory.util.GeoUtils;
import workflow.utils.ClusterPoint;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

public class KDistUtility {
	
	private static double[] getElbowPoint(List<Double> distances, int A, int B) {
		boolean sorted = Ordering.natural().reverse().isOrdered(distances);
		if (!sorted)
			throw new RuntimeException("La lista deve essere ordinata in ordine decrescente");
		
		if (B - A < 2)
			throw new RuntimeException("Tra A e B ci deve almeno essere un elemento");
		int dim = B - A + 1;

		// max variables
		int iMax = 0;
		double distMax = 0;
		double xMax = 0;
		double yMax = 0;

		// temporary variables
		double xNorm = -1;
		double yNorm = -1;
		double tmpDist = 0;

		double yB = distances.get(B);
		double yA = distances.get(A);
		double yI = 0.0d;

		double xB = B;
		double xA = A;
		double xI = 0.0d;

		int i = 0;

		for (Iterator<Double> it = distances.iterator(); it.hasNext();) {
			yI = it.next();
			if (i == A) {
				distMax = 0;
				iMax = i;
				xMax = i;
				yMax = yI;
			} else if (i > A && i <= B) {
				xI = i;
				xNorm = (xI - xA) / (xB - xA);
				yNorm = (yI - yB) / (yA - yB);

				tmpDist = ((1.0 - yNorm) - xNorm) * Math.sqrt(2.0) / 2.0;

				if (tmpDist > distMax) {
					distMax = tmpDist;
					iMax = i;
					xMax = xI;
					yMax = yI;
				}
			}
			i++;
		}
		return new double[] { iMax, distMax, xMax, yMax };
	}
	
	private static double[] getElbowPoint(List<Double> distances, int A, int B, double threshold_perc) {
		boolean sorted = Ordering.natural().reverse().isOrdered(distances);
		if (!sorted)
			throw new RuntimeException("La lista deve essere ordinata in ordine decrescente");
		
		if (B - A < 2)
			throw new RuntimeException("Tra A e B ci deve almeno essere un elemento");
		int dim = B - A + 1;

		// max variables
		int iMax = 0;
		double distMax = 0;
		double xMax = 0;
		double yMax = 0;

		// temporary variables
		double xNorm = -1;
		double yNorm = -1;
		double tmpDist = 0;

		double yB = distances.get(B);
		double yA = distances.get(A);
		double yI = 0.0d;

		double xB = B;
		double xA = A;
		double xI = 0.0d;

		int i = 0;

		for (Iterator<Double> it = distances.iterator(); it.hasNext();) {
			yI = it.next();
			if (i == A) {
				distMax = 0;
				iMax = i;
				xMax = i;
				yMax = yI;
			} else if (i > A && i <= B) {
				xI = i;
				xNorm = (xI - xA) / (xB - xA);
				yNorm = (yI - yB) / (yA - yB);

				tmpDist = ((1.0 - yNorm) - xNorm) * Math.sqrt(2.0) / 2.0;

				if (yNorm < (1 - threshold_perc - xNorm)) {
					if (tmpDist > distMax) {
						distMax = tmpDist;
						iMax = i;
						xMax = xI;
						yMax = yI;
					}
				}
			}
			i++;
		}
		return new double[] { iMax, distMax, xMax, yMax };
	}
	
	public static double calculateFirstEps(List<Double> distances) {
		return calculateFirstEps("", distances);
	}

	public static double calculateFirstEps(String name, List<Double> distances) {
		double[] ret = getElbowPoint(distances, 0, distances.size() - 1);

		double perCut = 1.0 * ret[0] / distances.size();
		String perCutS = "" + (perCut * 100);

		System.out.println(name + " -> Elbow line= " + ((int) ret[0]) + "/" + distances.size() + " con distanza "
				+ ret[3] + " . Taglio del " + perCutS.substring(0, perCutS.indexOf(".") + 2) + "%");
		return ret[3];
	}
	
	public static double calculateFixedEps(String name, List<Double> distances, double perc) {
		if (perc < 0 || perc > 1)
			throw new RuntimeException("perc deve essere compreso tra 0  e 1");

		int pos = (int) (distances.size() * perc);

		double perCut = pos * 1.0 / distances.size();
		String perCutS = "" + (perCut * 100);

		System.out.println(name + " -> Elbow line= " + (pos) + "/" + distances.size() + " con distanza "
				+ distances.get(pos) + " . Taglio del " + perCutS.substring(0, perCutS.indexOf(".") + 2) + "%");
		return distances.get(pos);
	}

	public static double calculateFixedEpsFast(String name, List<Double> distances, double threshold_perc) {
		int iRet = 0;

		int N = Math.min(100, distances.size());
		int step = (distances.size() - 2) / N;
		int iMax = 0;

		for (int i = 0; i <= distances.size() - 2 - 1;) {
			boolean ret = isElbowPointFar(distances, i, distances.size() - 1, threshold_perc);
			if (ret) {
				iMax = i;
				// System.out.println(i+"\t"+iMax+"\t"+ret);
			} else {
				break;
			}
			if (i == distances.size() - 2 - 1)
				break;
			i += step;
			if (i > distances.size() - 2 - 1)
				i = distances.size() - 2 - 1;
		}

//		System.out.print("Salto "+iMax+" . ");

		for (int i = iMax; i < distances.size() - 2; i++) {
			boolean ret = isElbowPointFar(distances, i, distances.size() - 1, threshold_perc);
			// System.out.println(i+"\t"+ret[0]+"\t"+ret[1]/(Math.sqrt(2)/2));
			if (!ret) {
				iRet = i;
				break;
			}
		}

		double perCut = 1.0 * iRet / distances.size();
		String perCutS = "" + (perCut * 100);

//		System.out.println(name + " -> Elbow line= " + ((int) iRet) + "/" + distances.size() + " con distanza >"
//				+ threshold_perc + " . Taglio del " + perCutS.substring(0, perCutS.indexOf(".") + 2) + "%");
		return distances.get(iRet);
	}
	
	public static double calculateEpsWithThreshold(String name, List<Double> distances, double threshold_perc) {
		if (threshold_perc < 0 || threshold_perc > 1)
			throw new RuntimeException("perc deve essere compreso tra 0  e 1");
		int iRet = 0;

		for (int i = 0; i < distances.size() - 2; i++) {
			boolean ret = isElbowPointFar(distances, i, distances.size() - 1, threshold_perc);
			// System.out.println(i+"\t"+ret[0]+"\t"+ret[1]/(Math.sqrt(2)/2));
			if (!ret) {
				iRet = i;
				break;
			}
		}

		double perCut = 1.0 * iRet / distances.size();
		String perCutS = "" + (perCut * 100);

//	System.out.println(name + " -> Elbow line= " + ((int) iRet) + "/" + distances.size() + " con distanza_perc <="
//			+ threshold_perc + " . Taglio del " + perCutS.substring(0, perCutS.indexOf(".") + 2) + "%");
		return distances.get(iRet);
	}

	public static double calculateEpsAuto(String name, List<Double> list, int numRip, int numSampl) {
		Double[] distances = list.toArray(new Double[list.size()]);
		
		List<Double> temp;
		LinkedList<Double> res = new LinkedList<>();
		Random ran = new Random();
		
		for(int rip=0; rip<numRip; rip++) {
			temp = new LinkedList<Double>();
			for(int i=0; i<numSampl; i++) {
				temp.add(distances[ran.nextInt(distances.length)]);
			}
			Collections.sort(temp, Collections.reverseOrder());
			//System.out.println(temp);
			double eps = KDistUtility.calculateFirstEps(name,temp);
			//System.out.println(first);
			res.add(eps);
		}
		Collections.sort(res);
		double mean = Stats.meanOf(res);
		double median = Quantiles.median().compute(res);
		
		System.out.println("Mean "+mean+" ,median "+median);
		return median;
	}

//	public static double calculateEpsAuto(String name, List<Double> distances) {
//		System.out.println(distances.size());
//		int N = Math.min(20, distances.size());
//		int step = (distances.size() - 2) / N;
//		int iMax = -1;
//		double distNorm = 1;
//		for (int i = 0; i <= distances.size() - 2 - 1;) {
//			double[] ret = getElbowPoint(distances, i, distances.size() - 1);
//			if (ret[1] <= distNorm * 1.00) {
//				distNorm = ret[1];
//				iMax = i;
//				System.out.println(i + "\t" + iMax + "\t" + distNorm);
//			} else {
//				break;
//			}
//			if (i == distances.size() - 2 - 1)
//				break;
//			i += step;
//			if (i > distances.size() - 2 - 1)
//				i = distances.size() - 2 - 1;
//		}
//
//		double perCut = 1.0 * iMax / distances.size();
//		String perCutS = "" + (perCut * 100);
//
//		System.out.println(name + " -> Elbow line= " + ((int) iMax) + "/" + distances.size() + " con distanza "
//				+ distNorm + " . Taglio del " + perCutS.substring(0, perCutS.indexOf(".") + 2) + "%");
//
//		return distances.get(iMax);
//	}

	public static boolean isElbowPointFar(List<Double> distances, int A, int B, double threshold_perc) {

		if (B - A < 2)
			throw new RuntimeException("Tra A e B ci deve almeno essere un elemento");
		int dim = B - A + 1;

		// temporary variables
		double xNorm = -1;
		double yNorm = -1;
		double tmpDist = 0;

		double yB = distances.get(B);
		double yA = distances.get(A);
		double yI = 0.0d;

		double xB = B;
		double xA = A;
		double xI = 0.0d;

		int i = 0;

		for (Iterator<Double> it = distances.iterator(); it.hasNext();) {
			yI = it.next();
			if (i > A && i < B) {
				xI = i;
				xNorm = (xI - xA) / (xB - xA);
				yNorm = (yI - yB) / (yA - yB);

				if (yNorm < (1 - threshold_perc - xNorm)) {
					return true;
				}
			}
			i++;
		}
		return false;
	}

	public static List<Double> calculateKNN(Collection<ClusterPoint> points, int numNeighbours) {

		List<Double> ret = new LinkedList<Double>();
		double[] mins = new double[numNeighbours];

		double distance = 0.0, tmp = 0.0, tmp2 = 0.0;

		for (Point p1 : points) {
			// reset mins
			for (int i = 0; i < mins.length; i++) {
				mins[i] = Double.MAX_VALUE;
			}

			for (Point p2 : points) {
				if (!p1.equals(p2)) {
					distance = GeoUtils.distance(p1, p2);
					cycle: for (int i = 0; i < mins.length; i++) {
						if (distance < mins[i]) {
							tmp = distance;
							for (int j = i; j < mins.length; j++) {
								tmp2 = mins[j];
								mins[j] = tmp;
								tmp = tmp2;
							} // swap
							mins[i] = distance;
							break cycle;
						} // if con immissione
					} // cycle
				} // if
			} // for p2
			ret.add(mins[numNeighbours - 1]);
		}
		Collections.sort(ret, Collections.reverseOrder());
		return ret;
	}

	public static double round(double value, int places) {
		if (places < 0)
			throw new IllegalArgumentException();

		BigDecimal bd = new BigDecimal(value);
		bd = bd.setScale(places, RoundingMode.HALF_UP);
		return bd.doubleValue();
	}
	
//	public static double calculateEpsWithThresholdOld(String name, List<Double> distances, double threshold_perc) {
//	double[] iRet = null;
//
//	int N = Math.min(100, distances.size());
//	int step = (distances.size() - 2) / N;
//	int iMax = 0;
//
//	for (int i = 0; i <= distances.size() - 2 - 1;) {
//		double[] ret = getElbowPoint(distances, i, distances.size() - 1, threshold_perc);
//		if (ret[1] > threshold_perc) {
//			iMax = i;
//			System.out.println(i + "\t" + iMax + "\t" + ret[1]);
//		} else {
//			break;
//		}
//		if (i == distances.size() - 2 - 1)
//			break;
//		i += step;
//		if (i > distances.size() - 2 - 1)
//			i = distances.size() - 2 - 1;
//	}
//
//	System.out.println("Salto " + iMax);
//
//	for (int i = iMax; i < distances.size() - 2; i++) {
//		double[] ret = getElbowPoint(distances, i, distances.size() - 1, threshold_perc);
//		// System.out.println(i+"\t"+ret[0]+"\t"+ret[1]/(Math.sqrt(2)/2));
//		if (ret[0] == i) {
//			iRet = ret;
//			break;
//		}
//	}
//
//	double perCut = 1.0 * iRet[0] / distances.size();
//	String perCutS = "" + (perCut * 100);
//
//	System.out.println(name + " -> Elbow line= " + ((int) iRet[0]) + "/" + distances.size() + " con distanza "
//			+ iRet[3] + " . Taglio del " + perCutS.substring(0, perCutS.indexOf(".") + 2) + "%");
//	return iRet[3];
//}
	
//	public static List<Double> calculateKNN(Collection<ClusterPoint> points, int numNeighbours) {
//
//		List<Double> ret = new LinkedList<Double>();
//		List<Double> tmp = new LinkedList<Double>();
//
//		double distance = 0.0;
//		for (Point p1 : points) {
//			tmp.clear();
//			for (Point p2 : points) {
//				if (!p1.equals(p2)) {
//					distance = GeoUtils.distance(p1, p2);
//					tmp.add(distance);
//				}
//			}
//			Collections.sort(tmp);
//			ret.add(tmp.get(numNeighbours-1));
//		}
//		Collections.sort(ret, Collections.reverseOrder());
//
//		return ret;
//	}
	

	
//	public static double calculateEpsAutoOld(String name, List<Double> distances) {
//		System.out.println(distances.size());
//		int N = Math.min(100, distances.size());
//		int step = (distances.size() - 2) / N;
//		int iMax = -1;
//		double distNorm = 1;
//		for (int i = 0; i <= distances.size() - 2 - 1;) {
//			double[] ret = getElbowPoint(distances, i, distances.size() - 1);
//			if (ret[1] <= distNorm) {
//				distNorm = ret[1];
//				iMax = i;
//				System.out.println(i + "\t" + iMax + "\t" + distNorm);
//			} else {
//				break;
//			}
//			if (i == distances.size() - 2 - 1)
//				break;
//			i += step;
//			if (i > distances.size() - 2 - 1)
//				i = distances.size() - 2 - 1;
//		}
//
//		LinkedList<Double> normDist = new LinkedList<>();
//
//		for (int i = 0; i <= iMax; i++) {
//			double[] ret = getElbowPoint(distances, i, distances.size() - 1);
//			normDist.add(ret[1]);
//		}
//
//		double[] iRet = getElbowPoint(normDist, 0, normDist.size() - 1);
//
//		double perCut = 1.0 * iRet[0] / distances.size();
//		String perCutS = "" + (perCut * 100);
//
//		System.out.println(name + " -> Elbow line= " + ((int) iRet[0]) + "/" + distances.size() + " con distanza "
//				+ iRet[3] + " . Taglio del " + perCutS.substring(0, perCutS.indexOf(".") + 2) + "%");
//
//		return distances.get((int) iRet[0]);
//	}
//
//	public static double calculateEpsAutoTutto(String name, List<Double> distances) {
//
//		LinkedList<Double> normDist = new LinkedList<>();
//
//		for (int i = 0; i <= distances.size() - 2 - 1; i++) {
//			double[] ret = getElbowPoint(distances, i, distances.size() - 1);
//			normDist.add(ret[1]);
//		}
//
//		double[] iRet = getElbowPoint(normDist, 0, normDist.size() - 1);
//
//		double perCut = 1.0 * iRet[0] / distances.size();
//		String perCutS = "" + (perCut * 100);
//
//		System.out.println(name + " -> Elbow line= " + ((int) iRet[0]) + "/" + distances.size() + " con distanza "
//				+ iRet[3] + " . Taglio del " + perCutS.substring(0, perCutS.indexOf(".") + 2) + "%");
//
//		return distances.get((int) iRet[0]);
//	}

	public static void main(String[] args) {
		System.out.println("Here");
	}
}
