package spark.wiki.linked.triple.stats.sample;

import avroschema.linked.TripleLinked;
import avroschema.util.AvroUtils;
import avroschema.util.TripleLinkedUtils;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class SampleFromHighConfTriplesToText {
    /**
     *
     * @author Kiril Gashteovski
     *
     * Take a sample triple from OPIEC
     */
    private static JavaSparkContext context;

    public static void main(String args[]) {
        // Initializations
        String OPIEC_DIR = args[0];
        String WRITE_DIR = args[1];
        int sampleSize = Integer.parseInt(args[2]);
        double confThreshold = Double.parseDouble(args[3]);

        SparkConf conf = new SparkConf().setAppName("WikiTriplesLinked").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> wikiTriplesLinkedPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(OPIEC_DIR, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        JavaRDD<String> highConfTriplesRDD = wikiTriplesLinkedPairRDD.map(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            if (triple.getConfidenceScore() > confThreshold) {
                return TripleLinkedUtils.tripleLinkedToString(triple);
            } else {
                return null;
            }
        }).filter(a -> a != null);

        // Take the sample
        List<String> tripleSampleList = highConfTriplesRDD.takeSample(false, sampleSize);
        JavaRDD<String> tripleSampleRDD = context.parallelize(tripleSampleList).coalesce(1);
        tripleSampleRDD.saveAsTextFile(WRITE_DIR);
    }
}
