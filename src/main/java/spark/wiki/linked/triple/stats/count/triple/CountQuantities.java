package spark.wiki.linked.triple.stats.count.triple;

import avroschema.linked.TripleLinked;
import avroschema.util.AvroUtils;
import avroschema.util.Utils;
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
 * @author Kiril Gashteovski
 */

public class CountQuantities {
    private static JavaSparkContext context;

    public static void main(String args[]) {
        // Initializations
        String linkedTriplesReadDir = args[0];
        String linkedTriplesWriteDir = args[1];
        SparkConf conf = new SparkConf().setAppName("Table1").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> wikiArticlesNLPPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(linkedTriplesReadDir, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        JavaPairRDD<String, Integer> quantityTriplesCountRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            if (Utils.hasQuantity(triple)) {
                return new Tuple2<>("HAS_QUANTITY", 1);
            } else {
                return new Tuple2<>("HAS_NO_QUANTITY", 1);
            }
        }).reduceByKey((a, b) -> (a + b));

        quantityTriplesCountRDD.saveAsTextFile(linkedTriplesWriteDir);
    }
}
