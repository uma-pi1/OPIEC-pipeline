package spark.wiki.linked.triple.annotate;

import avroschema.linked.TripleLinked;
import avroschema.linked.WikiArticleLinkedNLP;
import avroschema.util.AvroUtils;
import avroschema.util.TripleLinkedUtils;
import de.uni_mannheim.utils.RedirectLinksMap;
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
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author Kiril Gashteovski
 */

public class PopulateCanonicalLinks {
    private static JavaSparkContext context;

    public static void main(String args[]) throws IOException {
        // Initializations
        String inputDirectory = args[0];
        String outputDirectory = args[1];

        // Initializations
        SparkConf conf = new SparkConf().setAppName("Triples").remove("spark.serializer");
        context = new JavaSparkContext(conf);
        Schema schema = AvroUtils.toSchema(WikiArticleLinkedNLP.class.getName());
        Job inputJob = AvroUtils.getJobInputKeyAvroSchema(schema);

        // Load dictionary of redirection links
        RedirectLinksMap REDIRECT_LINKS_MAP = new RedirectLinksMap("/redirects/redirects.dict");
        Broadcast<HashMap<String, String>> REDIRECT_LINKS_MAP_BROADCAST = context.broadcast(REDIRECT_LINKS_MAP.getRedirectMap());

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> wikiArticlesNLPPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(inputDirectory, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        JavaRDD<TripleLinked> triplesRDD = wikiArticlesNLPPairRDD.map(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            triple.setCanonicalLinks(TripleLinkedUtils.getCanonicalLinksMap(triple, REDIRECT_LINKS_MAP_BROADCAST.getValue()));
            return triple;
        });

        // Serializing to AVRO
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> javaPairRDD = triplesRDD.mapToPair(r -> new Tuple2<>(new AvroKey<>(r), NullWritable.get()));
        Job job = AvroUtils.getJobOutputKeyAvroSchema(TripleLinked.getClassSchema());
        javaPairRDD.saveAsNewAPIHadoopFile(outputDirectory, AvroKey.class, NullWritable.class,
                AvroKeyOutputFormat.class, job.getConfiguration());
    }
}
