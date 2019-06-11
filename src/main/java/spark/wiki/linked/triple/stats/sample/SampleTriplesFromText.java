package spark.wiki.linked.triple.stats.sample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class SampleTriplesFromText {
    /**
     * @author Kiril Gashteovski
     *
     * Take a sample triple from text triples
     */
    private static JavaSparkContext context;

    public static void main(String args[]) {
        // Initializations
        String textTriplesDir = args[0];
        String writeDir = args[1];
        int sampleSize = Integer.parseInt(args[2]);

        SparkConf conf = new SparkConf().setAppName("WikiTriplesLinked").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        List<String> sample = context.textFile(textTriplesDir).takeSample(false, sampleSize);
        JavaRDD<String> sampleRDD = context.parallelize(sample).coalesce(1);
        sampleRDD.saveAsTextFile(writeDir);
    }
}
