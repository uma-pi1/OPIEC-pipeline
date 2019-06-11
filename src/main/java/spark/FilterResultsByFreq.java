package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author Kiril Gashteovski
 */
public class FilterResultsByFreq {
    private static JavaSparkContext context;
    public static void main(String args[]) {
        // Input parameters (triples directory,
        String inputDirectory = args[0];
        String outputDirectory = args[1];
        int minfreq = Integer.parseInt(args[2]);

        // Spark initializations
        SparkConf conf = new SparkConf().setAppName("FilterResultsByFrequency").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Read counts files and filter out the "infrequent ones"
        JavaRDD<String> countsRDD = context.textFile(inputDirectory);
        JavaPairRDD<String, Integer> frequentCountPairsRDD = countsRDD.mapToPair(countline -> {
            String [] lineSplit = countline.split(",");
            int count = Integer.parseInt(lineSplit[lineSplit.length-1].substring(0, lineSplit[lineSplit.length-1].length()-1));
            String tempKey = String.join(",", Arrays.copyOfRange(lineSplit, 0, lineSplit.length-1));
            String finalKey = tempKey.substring(1, tempKey.length());
            return new Tuple2<>(finalKey, count);
        }).filter(pair -> pair._2() >= minfreq).coalesce(1);

        // Write to disk and close context
        frequentCountPairsRDD.saveAsTextFile(outputDirectory);
        context.close();
    }
}
