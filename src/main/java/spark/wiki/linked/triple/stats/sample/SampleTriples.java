package spark.wiki.linked.triple.stats.sample;

import avroschema.linked.TripleLinked;
import avroschema.util.AvroUtils;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.List;

public class SampleTriples {
    /**
     * @author Kiril Gashteovski
     *
     * Take a sample triple from the unique SPOs.
     */
    private static JavaSparkContext context;

    public static void main(String args[]) {
        // Initializations
        String linkedTriplesReadDir = args[0];
        String linkedTriplesWriteDir = args[1];
        int sampleSize = Integer.parseInt(args[2]);

        SparkConf conf = new SparkConf().setAppName("WikiTriplesLinked").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> wikiTriplesLinkedPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(linkedTriplesReadDir, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        // Take the sample
        List<TripleLinked> tripleSampleList = wikiTriplesLinkedPairRDD.map(tuple -> AvroUtils.cloneAvroRecord(tuple._1().datum()))
                .takeSample(false, sampleSize);
        JavaRDD<TripleLinked> tripleSampleRDD = context.parallelize(tripleSampleList);

        // Serializing to AVRO
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> javaPairRDD = tripleSampleRDD.mapToPair(r -> new Tuple2<>(new AvroKey<>(r), NullWritable.get()));
        Job job = AvroUtils.getJobOutputKeyAvroSchema(TripleLinked.getClassSchema());
        javaPairRDD.saveAsNewAPIHadoopFile(linkedTriplesWriteDir, AvroKey.class, NullWritable.class,
                AvroKeyOutputFormat.class, job.getConfiguration());
    }
}
