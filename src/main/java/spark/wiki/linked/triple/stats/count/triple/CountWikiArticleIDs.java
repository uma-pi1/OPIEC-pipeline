package spark.wiki.linked.triple.stats.count.triple;

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
 * @author Kiril Gashteovski
 */

public class CountWikiArticleIDs {
    private static JavaSparkContext context;

    public static void main(String args[]) {
        // Initializations
        String TRIPLES_LINKED_DIR = args[0];
        String WRITE_DIR = args[1];
        SparkConf conf = new SparkConf().setAppName("WikiTriplesLinked").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> wikiTriplesPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(TRIPLES_LINKED_DIR, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        JavaPairRDD<String, Integer> WikiArticleIDCountsRDD = wikiTriplesPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            return new Tuple2<>(triple.getArticleId(), 1);
        }).reduceByKey((a, b) -> (a + b))
                .coalesce(1);

        WikiArticleIDCountsRDD.saveAsTextFile(WRITE_DIR);
    }
}
