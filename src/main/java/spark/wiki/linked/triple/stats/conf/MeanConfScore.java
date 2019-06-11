package spark.wiki.linked.triple.stats.conf;

import avroschema.linked.TripleLinked;
import avroschema.util.AvroUtils;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Kiril Gashteovski
 *
 *  NOTE: used for Table 2
 **/

public class MeanConfScore {
    private static JavaSparkContext context;

    public static void main(String args[]) {
        // Initializations
        String linkedTriplesReadDir = args[0];
        String WRITE_DIR = args[1];

        SparkConf conf = new SparkConf().setAppName("MeanConfScore").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> wikiTriplesLinkedPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(linkedTriplesReadDir, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        JavaDoubleRDD confScoresRDD = wikiTriplesLinkedPairRDD.mapToDouble(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            return triple.getConfidenceScore();
        });

        // Compute everything and save it in a file
        double meanConf = confScoresRDD.mean();
        double stDevConf = confScoresRDD.stdev();
        StatCounter stats = confScoresRDD.stats();
        List<String> statsStringList = new ArrayList<>();
        statsStringList.add("Mean conf. = " + Double.toString(meanConf));
        statsStringList.add("St. dev. conf. = " + Double.toString(stDevConf));
        statsStringList.add("Full stats: " + stats.toString());
        JavaRDD<String> statsRDD = context.parallelize(statsStringList).coalesce(1);
        statsRDD.saveAsTextFile(WRITE_DIR);
    }
}
