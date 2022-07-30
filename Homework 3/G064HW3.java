//Group 064
//Edoardo Bastianello, Stefano Binotto, Giulia Pisacreta

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class G064HW3
{

// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
// MAIN PROGRAM 
// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

  public static void main(String[] args) throws Exception {

      if (args.length != 4) {
          throw new IllegalArgumentException("USAGE: filepath k z L");
      }

      // ----- Initialize variables 
      String filename = args[0];
      int k = Integer.parseInt(args[1]);
      int z = Integer.parseInt(args[2]);
      int L = Integer.parseInt(args[3]);
      long start, end; // variables for time measurements

      // ----- Set Spark Configuration
      Logger.getLogger("org").setLevel(Level.OFF);
      Logger.getLogger("akka").setLevel(Level.OFF);
      SparkConf conf = new SparkConf(true).setAppName("MR k-center with outliers");
      JavaSparkContext sc = new JavaSparkContext(conf);
      sc.setLogLevel("WARN");

      // ----- Read points from file
      start = System.currentTimeMillis();
      JavaRDD<Vector> inputPoints = sc.textFile(args[0], L)
              .map(x-> strToVector(x))
              .repartition(L)
              .cache();
      long N = inputPoints.count();
      end = System.currentTimeMillis();

      // ----- Pring input parameters
      System.out.println("File : " + filename);
      System.out.println("Number of points N = " + N);
      System.out.println("Number of centers k = " + k);
      System.out.println("Number of outliers z = " + z);
      System.out.println("Number of partitions L = " + L);
      System.out.println("Time to read from file: " + (end-start) + " ms");

      // ---- Solve the problem
      ArrayList<Vector> solution = MR_kCenterOutliers(inputPoints, k, z, L);

      // ---- Compute the value of the objective function
      start = System.currentTimeMillis();
      double objective = computeObjective(inputPoints, solution, z);
      end = System.currentTimeMillis();
      System.out.println("Objective function = " + objective);
      System.out.println("Time to compute objective function: " + (end-start) + " ms");

  }

// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
// AUXILIARY METHODS
// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
// Method strToVector: input reading
// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

  public static Vector strToVector(String str) {
      String[] tokens = str.split(",");
      double[] data = new double[tokens.length];
      for (int i=0; i<tokens.length; i++) {
        data[i] = Double.parseDouble(tokens[i]);
      }
      return Vectors.dense(data);
  }

// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
// Method euclidean: distance function
// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    public static double euclidean(Vector a, Vector b) {
        return Math.sqrt(Vectors.sqdist(a, b));
    }

// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
// Method MR_kCenterOutliers: MR algorithm for k-center with outliers 
// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

  public static ArrayList<Vector> MR_kCenterOutliers (JavaRDD<Vector> points, int k, int z, int L)
  {
        long start_r1, start_r2, end_r1, end_r2;

        //------------- ROUND 1 ---------------------------

        start_r1 = System.currentTimeMillis();
        JavaRDD<Tuple2<Vector,Long>> coreset = points.mapPartitions(x ->
        {
            ArrayList<Vector> partition = new ArrayList<>();
            while (x.hasNext()) partition.add(x.next());
            ArrayList<Vector> centers = kCenterFFT(partition, k+z+1);
            ArrayList<Long> weights = computeWeights(partition, centers);
            ArrayList<Tuple2<Vector,Long>> c_w = new ArrayList<>();
            for(int i =0; i < centers.size(); ++i)
            {
                Tuple2<Vector, Long> entry = new Tuple2<>(centers.get(i), weights.get(i));
                c_w.add(i,entry);
            }
            return c_w.iterator();
        }); // END OF ROUND 1

        //------------- ROUND 2 ---------------------------

        ArrayList<Tuple2<Vector, Long>> elems = new ArrayList<>((k+z)*L);
        elems.addAll(coreset.collect());
        end_r1 = System.currentTimeMillis();

        start_r2 = System.currentTimeMillis();
        ArrayList<Vector> P = new ArrayList<>(); //coordinates
        ArrayList<Long> W = new ArrayList<>(); //weights

        for (int i = 0; i < elems.size(); i++) {
            P.add((elems.get(i))._1());
            W.add((elems.get(i))._2());
        }

        int alpha = 2;

        ArrayList<Vector> solution = SeqWeightedOutliers(P, W, k, z, alpha);

        end_r2 = System.currentTimeMillis();

        System.out.println("Time Round 1: " + (end_r1-start_r1) + " ms");
        System.out.println("Time Round 2: " + (end_r2-start_r2) + " ms");

        return solution;
  }

// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
// Method kCenterFFT: Farthest-First Traversal
// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    public static ArrayList<Vector> kCenterFFT (ArrayList<Vector> points, int k) {

    final int n = points.size();
    double[] minDistances = new double[n];
    Arrays.fill(minDistances, Double.POSITIVE_INFINITY);

    ArrayList<Vector> centers = new ArrayList<>(k);

    Vector lastCenter = points.get(0);
    centers.add(lastCenter);
    double radius =0;

    for (int iter=1; iter<k; iter++) {
      int maxIdx = 0;
      double maxDist = 0;

      for (int i = 0; i < n; i++) {
        double d = euclidean(points.get(i), lastCenter);
        if (d < minDistances[i]) {
            minDistances[i] = d;
        }

        if (minDistances[i] > maxDist) {
            maxDist = minDistances[i];
            maxIdx = i;
        }
      }

      lastCenter = points.get(maxIdx);
      centers.add(lastCenter);
    }
    return centers;
}

// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
// Method computeWeights: compute weights of coreset points
// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    public static ArrayList<Long> computeWeights(ArrayList<Vector> points, ArrayList<Vector> centers)
    {
        Long weights[] = new Long[centers.size()];
        Arrays.fill(weights, 0L);
        for(int i = 0; i < points.size(); ++i)
        {
            double tmp = euclidean(points.get(i), centers.get(0));
            int mycenter = 0;
            for(int j = 1; j < centers.size(); ++j)
            {
                if(euclidean(points.get(i),centers.get(j)) < tmp)
                {
                    mycenter = j;
                    tmp = euclidean(points.get(i), centers.get(j));
                }
            }
            // System.out.println("Point = " + points.get(i) + " Center = " + centers.get(mycenter));
            weights[mycenter] += 1L;
        }
        ArrayList<Long> fin_weights = new ArrayList<>(Arrays.asList(weights));
        return fin_weights;
    }

// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
// Method SeqWeightedOutliers: sequential k-center with outliers
// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    public static ArrayList<Vector> SeqWeightedOutliers(ArrayList<Vector> P, ArrayList<Long>W, int k, int z, int alpha) {
        final int number_of_first_points = k+z+1;
        final int P_size = P.size();

        //matrix of distances
        double[][] distances = new double[P_size][P_size];

        //computation of distances
        for (int row = 0; row < P_size-1; row++) {
            for (int col = row+1; col < P_size; col++) {
                double d = Math.sqrt(Vectors.sqdist(P.get(row),P.get(col)));
                distances[row][col] = d;
                distances[col][row] = d;
            }
            distances[row][row] = 0;
        }
        distances[P_size-1][P_size-1] = 0;

        //minimum distance
        double d_min=(2^64)-1; //max value for a double var
        for (int row = 0; row < number_of_first_points; row++) {
            for (int col = 0; (col < number_of_first_points) && (col != row); col++) {
                double d = distances[row][col];
                if (d < d_min) {
                    d_min = d;
                }
            }
        }

        //initial guess
        final double r_min = d_min/2;
        double r = r_min;

        //initialization of counter of number of guesses
        int number_of_guesses = 0;

        //k-center OUT main loop
        while (true) {
            //update  number of guesses
            number_of_guesses++;

            ArrayList<Vector> Z = new ArrayList<Vector>(P);
            ArrayList<Vector> S = new ArrayList<>();

            //compute radius of smaller ball
            double bz_small = (1+(2*alpha))*r;

            //compute sum of weights Wz of not explored points
            long sum_of_weights = 0;
            for (int i = 0; i < W.size(); i++) {
                sum_of_weights += W.get(i);
            }

            while ((S.size() < k) && (sum_of_weights > 0)) {
                //initialization of local variables
                long max = 0;
                int index_new_center = -1;

                for (int row = 0; row < P_size; row++) {
                    long ball_weight = 0;

                    //compute Z elements not belonging to the small ball around x, and compute the sum of the weights ball-weight
                    for (int col = 0; col < Z.size(); col++) {
                        if ((Z.get(col) != null) && (distances[row][col] <= bz_small)) {
                            ball_weight += W.get(col);
                        }
                    }
                    //update max
                    if (ball_weight > max) {
                        max = ball_weight;
                        index_new_center = row;
                    }
                }

                //add new center to S
                S.add(P.get(index_new_center));

                //remove elements from Z and subtract their weights from Wz
                double bz_big = (3+(4*alpha))*r;
                for (int col = 0; col < Z.size(); col++) {
                    if ((Z.get(col) != null) && (distances[index_new_center][col] <= bz_big)) {
                        //set to 'null' explored elements
                        Z.set(col, null);
                        //update weight
                        long weight_y = W.get(col);
                        sum_of_weights -= weight_y;
                    }
                }
            }
            //termination condition
            if (sum_of_weights <= z) {
                System.out.println("Initial guess = " + r_min);
                System.out.println("Final guess = " + r);
                System.out.println("Number of guesses = " + number_of_guesses);

                //return centers
                return S;
            } else {
                //update guess
                r = 2*r;
            }
        }
    }

// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
// Method computeObjective: computes objective function  
// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
  public static double computeObjective (JavaRDD<Vector> points, ArrayList<Vector> centers, int z) {

      Double distance = points.mapPartitions(x -> {
          ArrayList<Double> part_distances = new ArrayList<>();

          //finding objective value
          while(x.hasNext()) {
              Vector tmp = x.next();
              double d_min = (2^64)-1;
              for (int j = 0; j < centers.size(); j++) {
                  double d = Math.sqrt(Vectors.sqdist(tmp, centers.get(j)));
                  if (d < d_min) {
                      d_min = d;
                  }
              }
              part_distances.add(d_min);
          }
          return part_distances.iterator();
      }).top(z+1) //sort in descending order and take the first z+1 distances (+1 because we want to store the farthest non-outlier point)
      .get(z); //because we want the non-outlier point distance

      return distance;
  }
}