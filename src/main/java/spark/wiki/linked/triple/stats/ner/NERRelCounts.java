package spark.wiki.linked.triple.stats.ner;

import avroschema.linked.TripleLinked;
import avroschema.util.AvroUtils;
import avroschema.util.TokenLinkedUtils;
import avroschema.util.TripleLinkedUtils;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

/**
 * @author Kiril Gashteovski
 *
 * NOTE: this script is used to compute content for Table 3
 */

public class NERRelCounts {
    private static JavaSparkContext context;

    public static void main(String args[]) {
        // Initializations
        String triplesReadDir = args[0];
        String countsWriteDir = args[1];
        SparkConf conf = new SparkConf().setAppName("NERCounts").remove("spark.serializer");
        conf.set("spark.driver.maxResultSize", "4g");

        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> triplesPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(triplesReadDir, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        // Argument pair NER counts
        JavaPairRDD<Tuple3<String, String, String>, Integer> nerRelsCount = triplesPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            String subjEntityType = TokenLinkedUtils.getNERIgnoringQuantities(triple.getSubject());
            String objEntityType = TokenLinkedUtils.getNERIgnoringQuantities(triple.getObject());
            return new Tuple2<>(new Tuple3<>(subjEntityType, objEntityType, TripleLinkedUtils.relToLemmatizedString(triple).toLowerCase()), 1);
        }).reduceByKey((a, b) -> (a + b))
                .filter(tuple -> tuple._2() >= 10)
                .coalesce(1);

        // Write files to disc
        nerRelsCount.saveAsTextFile(countsWriteDir);
    }
}
