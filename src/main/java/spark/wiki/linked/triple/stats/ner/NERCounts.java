package spark.wiki.linked.triple.stats.ner;

import avroschema.linked.TripleLinked;
import avroschema.util.AvroUtils;
import avroschema.util.TokenLinkedUtils;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

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
 * NOTE: this script is used for computing fig. 2 (a)
 */

public class NERCounts {
    private static JavaSparkContext context;

    public static void main(String [] args) {
        // Initializations
        String OPIEC_CLEAN_DIR = args[0];
        String pairCountsWriteDir = args[1];
        String entityCountsWriteDir = args[2];
        SparkConf conf = new SparkConf().setAppName("NERCounts").remove("spark.serializer");
        conf.set("spark.driver.maxResultSize", "4g");

        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> triplesPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(OPIEC_CLEAN_DIR, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        // Argument pair NER counts
        JavaPairRDD<String, Integer> nerPairsCount = triplesPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            String subjEntityType = TokenLinkedUtils.getNERIgnoringQuantities(triple.getSubject());
            String objEntityType = TokenLinkedUtils.getNERIgnoringQuantities(triple.getObject());
            return new Tuple2<>(subjEntityType + "\t" + objEntityType, 1);
        }).reduceByKey((a, b) -> (a + b)).coalesce(1);

        // Argument NER counts
        JavaPairRDD<String, Integer> argumentNERCounts = triplesPairRDD.flatMapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            ObjectArrayList<Tuple2<String, Integer>> ners = new ObjectArrayList<>();
            ners.add(new Tuple2<>(TokenLinkedUtils.getNERIgnoringQuantities(triple.getSubject()), 1));
            ners.add(new Tuple2<>(TokenLinkedUtils.getNERIgnoringQuantities(triple.getObject()), 1));
            return ners.iterator();
        }).reduceByKey((a, b) -> (a + b)).coalesce(1);

        // Write files to disc
        nerPairsCount.saveAsTextFile(pairCountsWriteDir);
        argumentNERCounts.saveAsTextFile(entityCountsWriteDir);
    }
}
