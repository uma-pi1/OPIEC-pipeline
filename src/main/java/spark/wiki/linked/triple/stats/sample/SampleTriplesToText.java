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

public class SampleTriplesToText {
    /**
     * @author Kiril Gashteovski
     *
     * Take a sample triple WA
     */
    private static JavaSparkContext context;

    public static void main(String args[]) {
        // Initializations
        String WA_DIR = args[0];
        String WRITE_DIR = args[1];
        int sampleSize = Integer.parseInt(args[2]);

        SparkConf conf = new SparkConf().setAppName("WikiTriplesLinked").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> wikiTriplesLinkedPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(WA_DIR, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        // Take the sample
        List<String> tripleSampleList = wikiTriplesLinkedPairRDD.map(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            return TripleLinkedUtils.tripleLinkedToString(triple);
        }).takeSample(false, sampleSize);
        JavaRDD<String> tripleSampleRDD = context.parallelize(tripleSampleList).coalesce(1);
        tripleSampleRDD.saveAsTextFile(WRITE_DIR);
    }
}
