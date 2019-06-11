package spark.wiki.linked.triple.stats.conf;

import avroschema.linked.TripleLinked;
import avroschema.util.AvroUtils;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 *
 * @author Kiril Gashteovski
 *
 * NOTE: This script is used for Fig. 3(b)
 *
 **/

public class ConfScoreIntervalFrequencies {
    private static JavaSparkContext context;

    public static void main(String args[]){
        // Initializations
        String READ_DIR = args[0];
        String WRITE_DIR_INTERVAL_FREQS = args[1];
        String WRITE_DIR_FINE_GRAINED_FREQS = args[2];

        SparkConf conf = new SparkConf().setAppName("ConfScoreIntervalFrequencies").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> wikiTriplesLinkedPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(READ_DIR, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        JavaPairRDD<String, Integer> confScoresIntervalFreqsRDD = wikiTriplesLinkedPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            if (triple.getConfidenceScore() < 0.1) {
                return new Tuple2<>("[0, 0.1)", 1);
            } else if (triple.getConfidenceScore() >= 0.1 && triple.getConfidenceScore() < 0.2) {
                return new Tuple2<>("[0.1, 0.2)", 1);
            } else if (triple.getConfidenceScore() >= 0.2 && triple.getConfidenceScore() < 0.3) {
                return new Tuple2<>("[0.2, 0.3)", 1);
            } else if (triple.getConfidenceScore() >= 0.3 && triple.getConfidenceScore() < 0.4) {
                return new Tuple2<>("[0.3, 0.4)", 1);
            } else if (triple.getConfidenceScore() >= 0.4 && triple.getConfidenceScore() < 0.5) {
                return new Tuple2<>("[0.4, 0.5)", 1);
            } else if (triple.getConfidenceScore() >= 0.5 && triple.getConfidenceScore() < 0.6) {
                return new Tuple2<>("[0.5, 0.6)", 1);
            } else if (triple.getConfidenceScore() >= 0.6 && triple.getConfidenceScore() < 0.7) {
                return new Tuple2<>("[0.6, 0.7)", 1);
            } else if (triple.getConfidenceScore() >= 0.7 && triple.getConfidenceScore() < 0.8) {
                return new Tuple2<>("[0.7, 0.8)", 1);
            } else if (triple.getConfidenceScore() >= 0.8 && triple.getConfidenceScore() < 0.9) {
                return new Tuple2<>("[0.8, 0.9)", 1);
            } else {
                return new Tuple2<>("[0.9, 1.0]", 1);
            }
        }).reduceByKey((a, b) -> a+b).coalesce(1);
        confScoresIntervalFreqsRDD.saveAsTextFile(WRITE_DIR_INTERVAL_FREQS);

        // Fine grained frequencies
        JavaPairRDD<Double, Integer> confScoresFreqsFineGrainedRDD = wikiTriplesLinkedPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            return new Tuple2<>(triple.getConfidenceScore(), 1);
        }).reduceByKey((a, b) -> a+b).coalesce(1);
        confScoresFreqsFineGrainedRDD.saveAsTextFile(WRITE_DIR_FINE_GRAINED_FREQS);
    }
}
