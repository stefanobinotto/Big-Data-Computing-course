//Group 064
//Edoardo Bastianello, Stefano Binotto, Giulia Pisacreta

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.lang.Math;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;


public class G064HW2{

  //main
  public static void main(String[] args) throws IOException {
    // CHECKING NUMBER OF CMD LINE PARAMETERS
    if (args.length != 3) {
      throw new IllegalArgumentException("USAGE: file_path number_of_centers number_of_outliers");
    }

    //Command Line arguments
    String path = args[0];
    //hyperparameters
    int k = Integer.parseInt(args[1]);
    int z = Integer.parseInt(args[2]);
    int alpha = 0;

    ArrayList<Vector> inputPoints = readVectorsSeq(path);

    // weights initialization
    ArrayList<Long> weights = new ArrayList<>();
    long init_w = 1L;
    for (int i = 0; i < inputPoints.size(); i++) {
      weights.add(init_w);
    }

    System.out.println("Input size n = " + inputPoints.size());
    System.out.println("Number of centers k = " + k);
    System.out.println("Number of outliers z = " + z);

    long pre_time = System.currentTimeMillis();
    ArrayList<Vector> solution = SeqWeightedOutliers(inputPoints, weights, k, z, alpha);
    long post_time = System.currentTimeMillis();

    double objective = ComputeObjective(inputPoints,solution,z);

    System.out.println("Objective function = " + objective);
    System.out.println("Time of SeqWeightedOutliers = " + (post_time-pre_time));
  }


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

  public static double ComputeObjective(ArrayList<Vector> inputPoints, ArrayList<Vector> solution, int z) {
    ArrayList<Double> distances = new ArrayList<Double>();

    //finding objective value
    for (int i = 0; i < inputPoints.size(); i++) {
      Vector tmp = inputPoints.get(i);
      double d_min = (2^64)-1;
      for (int j = 0; j < solution.size(); j++) {
        double d = Math.sqrt(Vectors.sqdist(tmp, solution.get(j)));
        if (d < d_min) {
          d_min = d;
        }
      }
      distances.add(d_min);
    }
    //sort distances
    Collections.sort(distances);

    return distances.get(distances.size()-z-1);
  }

  // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
  // Input reading methods
  // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
  public static Vector strToVector(String str) {
    String[] tokens = str.split(",");
    double[] data = new double[tokens.length];
    for (int i = 0; i < tokens.length; i++) {
      data[i] = Double.parseDouble(tokens[i]);
    }
    return Vectors.dense(data);
  }

  public static ArrayList<Vector> readVectorsSeq(String filename) throws IOException {
    if (Files.isDirectory(Paths.get(filename))) {
      throw new IllegalArgumentException("readVectorsSeq is meant to read a single file.");
    }
    ArrayList<Vector> result = new ArrayList<>();
    Files.lines(Paths.get(filename))
            .map(str -> strToVector(str))
            .forEach(e -> result.add(e));
    return result;
  }
}