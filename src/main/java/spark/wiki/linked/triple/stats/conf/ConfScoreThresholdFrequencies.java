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

import java.util.ArrayList;
import java.util.List;

/**
 * @author Kiril Gashteovski
 */

public class ConfScoreThresholdFrequencies {
    private static JavaSparkContext context;

    public static void main(String args[]){
        // Initializations
        String READ_DIR = args[0];
        String WRITE_DIR_INTERVAL_FREQS = args[1];

        SparkConf conf = new SparkConf().setAppName("ConfScoreIntervalFrequencies").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> wikiTriplesLinkedPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(READ_DIR, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        JavaPairRDD<String, Integer> confScoresIntervalFreqsRDD = wikiTriplesLinkedPairRDD.flatMapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            List<Tuple2<String, Integer>> tuples = new ArrayList<>();

            if (triple.getConfidenceScore() >= 0) {
                tuples.add(new Tuple2<>(">= 0", 1));
            }
            if (triple.getConfidenceScore() >= 0.1) {
                tuples.add(new Tuple2<>(">= 0.1", 1));
            }
            if (triple.getConfidenceScore() >= 0.2) {
                tuples.add(new Tuple2<>(">= 0.2", 1));
            }
            if (triple.getConfidenceScore() >= 0.3) {
                tuples.add(new Tuple2<>(">= 0.3", 1));
            }
            if (triple.getConfidenceScore() >= 0.4) {
                tuples.add(new Tuple2<>(">= 0.4", 1));
            }
            if (triple.getConfidenceScore() >= 0.5) {
                tuples.add(new Tuple2<>(">= 0.5", 1));
            }
            if (triple.getConfidenceScore() >= 0.6) {
                tuples.add(new Tuple2<>(">= 0.6", 1));
            }
            if (triple.getConfidenceScore() >= 0.7) {
                tuples.add(new Tuple2<>(">= 0.7", 1));
            }
            if (triple.getConfidenceScore() >= 0.8) {
                tuples.add(new Tuple2<>(">= 0.8", 1));
            }
            if (triple.getConfidenceScore() >= 0.9){
                tuples.add(new Tuple2<>(">= 0.9", 1));
            }

            return tuples.iterator();
        }).reduceByKey((a, b) -> a+b).coalesce(1);
        confScoresIntervalFreqsRDD.saveAsTextFile(WRITE_DIR_INTERVAL_FREQS);
    }
}
