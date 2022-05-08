//Group 064
//Edoardo Bastianello, Stefano Binotto, Giulia Pisacreta

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class G064HW1{

    //main
    public static void main(String[] args) throws IOException {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are:
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        if (args.length != 4)
        {
            throw new IllegalArgumentException("USAGE: num_partitions file_path");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        //
        // In the VM option, we set " -Dspark.master="local[*]" ": this sets up a Spark process
        // on the local machine, using the available cores for parallelism.
        // Alternatively, we could have use "yarn" instead of "local[*]" to run Spark on Yarn cluster manager,
        // which is the cluster manager used by the cloud computing platform available for the course.
        //
        // Rather than hardcoding the setting of the master in the code, one could pass it on the
        // command line (e.g., using the VM options in the Intellij interface for Java users).
        // This choice gives the flexibility of running the code on different architectures.
        //
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // We pass true to the SparkConf constructor.This has the effect that configuration
        // properties can also be passed on the command line.
        // Then, we set the name of the application as "WordCount"
        SparkConf conf = new SparkConf(true).setAppName("WordCount");

        // Based on the configuration object conf created above,
        // the Spark context is instantiated as follows:
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read number of partitions
        int K = Integer.parseInt(args[0]);

        //read H
        int H = Integer.parseInt(args[1]);

        //read country
        String S = args[2];

        //TASK 1
        // A way (but not the only one!) to read an input dataset is to store the dataset
        // as a text file, located at some path filepath which is passed as input to the program,
        // and then load the file into an RDD of strings, with each string corresponding to
        // a distinct line in the file.
        //
        // Read input file and subdivide it into K random partitions
        // cache() = upon occurrence of the action tries to store all data in main memory
        // rawData = RDD of Strings where each element is a line of the input file,
        // which is a transaction
        JavaRDD<String> rawData = sc.textFile(args[3]).repartition(K).cache();

        long numTransactions;
        //count total number of transactions
        numTransactions = rawData.count();
        //print the number of rows (transactions)
        System.out.println("Number of rows = " + numTransactions);

        //TASK 2
        // In Java , a dataset of key-value pairs with keys of type K and values of type V
        // is implemented through a JavaPairRDD<K,V> object, which is an RDD whose elements
        // are instances of the class Tuple2<K,V>. Given a pair T, instance Tuple2<K,V>, the methods T._1()
        // and T._2() return the key and the value of the pair, respectively.
        JavaPairRDD<String, Integer> productCustomer;

        // flatMapToPair(f): It applies function f passed as a parameter to each individual key-value pair of X,
        // transforming it into 0, 1 or more key-value pairs of type Tuple2<K',V'> (with arbitrary K' and V'),
        // which are returned as an iterator. The result is a JavaPairRDD<K',V'>.
        productCustomer = rawData.flatMapToPair((transaction) ->{
            /*
                For example the row: A536370,21883B,STAR GIFT TAPE, 24,2010-12-01 8:45,0.65,12583,France
                represents the fact that Transaction A536370 made by Customer 12583 on 12/1/2010
                at 8.45am contains 24 units of Product 21883B called Star Gift Tape.
            */

            // extract every token in every transaction (line) divided by " " (whitespace)
            String[] tokens = transaction.split(",");

            ArrayList<Tuple2<Tuple2<String, Integer>, Object>> pairs = new ArrayList<>();
            String productID = tokens[1];
            int quantity = Integer.parseInt(tokens[3]);
            int customerID = Integer.parseInt(tokens[6]);
            String country = tokens[7];

            if(quantity > 0 && (S.equals("all") || country.equals(S)))
            {
                Tuple2<String, Integer> t = new Tuple2<>(productID, customerID);
                Tuple2<Tuple2<String, Integer>, Object> t2 = new Tuple2<>(t, null);
                pairs.add(t2);
            }
            return pairs.iterator();

        // groupByKey(): for each key k occurring in X, it creates a key-value pair (k,w) where w
        // is an Iterable<V> containing all values of the key-value pairs with key k in X.
        // The result is a JavaPairRDD<K,Iterable<V>>. The reduce phase of MapReduce can be
        // implemented by applying flatMapToPair after groupByKey.
        // mapToPair(f): It applies function f passed as a parameter to each individual key-value pair of X,
        // transforming it into a key-value pair of type Tuple2<K',V'> (with arbitrary K' and V'). The result
        // is a JavaPairRDD<K',V'>. Note that the method cannot be used to eliminate elements of X,
        // and the returned RDD has the same number of elements as X. To filter out some elements from X,
        // one can invoke either the filter or the flatMapToPair methods described below.
        }).groupByKey().mapToPair((pair) ->{
                String prID = pair._1()._1();
                int cuID = pair._1()._2();
                return new Tuple2<>(prID, cuID);
                }
        );

        //print number of pairs in the RDD
        long numberOfDistinctPCPairs = productCustomer.count();
        System.out.println("Product-Customer Pairs = " + numberOfDistinctPCPairs);

        //TASK 3
        JavaPairRDD<String, Integer> productPopularity1;

        // mapPartitionsToPair(): It exploits the Spark partitioning.
        // It processes each partition's element provided as iterator
        productPopularity1 = productCustomer.mapPartitionsToPair((el) ->{
            ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
            int value = 1;
            while(el.hasNext())
            {
                Tuple2<String, Integer> newPair = el.next();
                pairs.add(new Tuple2<String, Integer>(newPair._1(), value));
            }
            return pairs.iterator();

        // mapValues(f): It transforms each key-value pair (k,v) in X into a
        // key-value pair (k,v'=f(v)) of type Tuple2<K,V'> (with arbitrary V') where f is
        // the function passed as a parameter. The result is a JavaPairRDD<K,V'>.
        }).groupByKey().mapValues((pairPC) ->{
            int sumPop = 0;
            for(int i : pairPC)
            {
                sumPop = sumPop + i;
            }
            return sumPop;
        });

        //TASK 4
        JavaPairRDD<String, Integer> productPopularity2;
        productPopularity2 = productCustomer.mapToPair((oldPair) ->{
            int value = 1;
            Tuple2<String, Integer> newPair = new Tuple2<>(oldPair._1(), value);
            return newPair;

        // ReduceByKey() is the combination of groupByKey() and mapValues()
        }).reduceByKey((x, y) -> x+y);

        //TASK 5
        if (H > 0) {
            JavaPairRDD<Integer, String> invertedPop;
            invertedPop = productPopularity1.mapToPair((oldPair) ->{
                Tuple2<Integer, String> newPair = new Tuple2<>(oldPair._2(), oldPair._1());
                return newPair;
            });

            // sortByKey(ascending): a transformation that can be applied when the elements of X are
            // key-value pairs (in Java X must be an JavaPairRDD). Given a boolean parameter ascending,
            // it sorts the elements of X by key in increasing order (ascending = true) or in decreasing
            // order (ascending = false). The parameter ascending is optional and, if missing, the default
            // is true. Calling collect() on the resulting RDD will output an ordered list
            // of key-value pairs (see example after collect()).
            JavaPairRDD<Integer, String> invertedPopSorted = invertedPop.sortByKey(false);

            List<Tuple2<Integer, String>> hPop = new ArrayList<>();
            // take(num): An action that brings the first num elements of X
            // into a list stored on the driver. When elements of X are key-value pairs,
            // calling take(num) after sortByKey return the first num pairs in the ordering.
            hPop = invertedPopSorted.take(H);
            List<Tuple2<String, Integer>> hPopFinal = new ArrayList<>();

            System.out.println("Top " + H + " Products and their Popularities");
            for(Tuple2<Integer, String> e: hPop) {
                int popularity = e._1();
                String prod = e._2();
                hPopFinal.add(new Tuple2<String, Integer>(prod, popularity));

                System.out.print("Product " + prod + " Popularity " +popularity + "; ");
            }
            System.out.println();
        }

        //TASK 6
        if (H == 0) {
            //productPopularity1
            System.out.println("productPopularity1:");
            for (Tuple2<String, Integer> tuple : productPopularity1.sortByKey(true).collect()) {
                System.out.print("Product: " + tuple._1() + " Popularity: " + tuple._2() + "; ");
            }
            System.out.println();
            //productPopularity2
            System.out.println("productPopularity2:");
            for (Tuple2<String, Integer> tuple : productPopularity2.sortByKey(true).collect()) {
                System.out.print("Product: " + tuple._1() + " Popularity: " + tuple._2() + "; ");
            }
        }
    }
}