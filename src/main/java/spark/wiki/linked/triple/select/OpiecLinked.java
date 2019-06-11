package spark.wiki.linked.triple.select;

import avroschema.linked.TripleLinked;
import avroschema.util.AvroUtils;
import avroschema.util.TokenLinkedUtils;

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

/**
 * @author Kiril Gashteovski
 */

public class OpiecLinked {
    /**
     * From all of the triples, select just the ones where both the subject and the object are linked arguments
     * NOTE: This is the script used for creating OPIEC-Linked
     */
    private static JavaSparkContext context;

    public static void main(String args[]) {
        // Initializations
        String OPIEC_CLEAN_DIR = args[0];
        String OPIEC_LINKED_DIR = args[1];
        SparkConf conf = new SparkConf().setAppName("WikiTriplesLinked").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> wikiArticlesNLPPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(OPIEC_CLEAN_DIR, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        JavaRDD<TripleLinked> triplesRDD = wikiArticlesNLPPairRDD.map(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());

            boolean subjIsOneLink = TokenLinkedUtils.allTokensAreOneLink(triple.getSubject());
            boolean subjIsQuantityWithOneLink = TokenLinkedUtils.allTokensAreQuantityWithOneLink(triple.getSubject());
            boolean objIsOneLink = TokenLinkedUtils.allTokensAreOneLink(triple.getObject());
            boolean objIsQuantityWithOneLink = TokenLinkedUtils.allTokensAreQuantityWithOneLink(triple.getObject());
            boolean subjPositiveCondition = subjIsOneLink || subjIsQuantityWithOneLink;
            boolean objPositiveCondition = objIsOneLink || objIsQuantityWithOneLink;

            if (subjPositiveCondition && objPositiveCondition) {
                return triple;
            } else {
                return null;
            }
        }).filter(a -> a != null);

        // Serializing to AVRO
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> javaPairRDD = triplesRDD.mapToPair(r -> new Tuple2<>(new AvroKey<>(r), NullWritable.get()));
        Job job = AvroUtils.getJobOutputKeyAvroSchema(TripleLinked.getClassSchema());
        javaPairRDD.saveAsNewAPIHadoopFile(OPIEC_LINKED_DIR, AvroKey.class, NullWritable.class,
                AvroKeyOutputFormat.class, job.getConfiguration());
    }
}
