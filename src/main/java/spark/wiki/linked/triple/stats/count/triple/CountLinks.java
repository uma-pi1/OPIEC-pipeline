package spark.wiki.linked.triple.stats.count.triple;

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
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Kiril Gashteovski
 */

public class CountLinks {
    private static JavaSparkContext context;

    public static void main(String args[]) throws IOException {
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

        JavaPairRDD<String, Integer> argumentLinkCountsRDD = wikiTriplesPairRDD.flatMapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            List<Tuple2<String, Integer>> links = new ArrayList<>();
            Tuple2<String, Integer> subjLink = new Tuple2(TripleLinkedUtils.getSubjLink(triple), 1);
            Tuple2<String, Integer> objLink = new Tuple2(TripleLinkedUtils.getObjLink(triple), 1);
            links.add(subjLink);
            links.add(objLink);
            return links.iterator();
        }).reduceByKey((a, b) -> (a + b))
                .coalesce(1);

        argumentLinkCountsRDD.saveAsTextFile(WRITE_DIR);
    }
}