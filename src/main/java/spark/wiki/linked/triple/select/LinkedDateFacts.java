package spark.wiki.linked.triple.select;

import avroschema.linked.TripleLinked;
import avroschema.util.AvroUtils;
import avroschema.util.TokenLinkedUtils;
import de.uni_mannheim.constant.NE_TYPE;
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

import java.util.ArrayList;
import java.util.List;

/**
 * @author Kiril Gashteovski
 */

public class LinkedDateFacts {
    private static JavaSparkContext context;

    public static void main(String args[]) {
        // Initializations
        String OPIEC_CLEAN_DIR = args[0];
        String WRITE_DIR = args[1];
        SparkConf conf = new SparkConf().setAppName("WikiTriplesLinked").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> wikiArticlesNLPPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(OPIEC_CLEAN_DIR, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        JavaRDD<TripleLinked> temporalLinkedTriplesRDD = wikiArticlesNLPPairRDD.flatMap(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            List<TripleLinked> triples = new ArrayList<>();

            boolean subjIsOneLink = TokenLinkedUtils.allTokensAreOneLink(triple.getSubject());
            boolean subjIsQuantityWithOneLink = TokenLinkedUtils.allTokensAreQuantityWithOneLink(triple.getSubject());

            if (subjIsOneLink || subjIsQuantityWithOneLink) {
                String objNer = TokenLinkedUtils.getNERIgnoringQuantities(triple.getObject());
                if (objNer.equals(NE_TYPE.DATE)) {
                    triples.add(triple);
                }
            }

            return triples.iterator();
        });

        // Serializing to AVRO
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> javaPairRDD = temporalLinkedTriplesRDD.mapToPair(r ->
                new Tuple2<>(new AvroKey<>(r), NullWritable.get()));
        Job job = AvroUtils.getJobOutputKeyAvroSchema(TripleLinked.getClassSchema());
        javaPairRDD.saveAsNewAPIHadoopFile(WRITE_DIR, AvroKey.class, NullWritable.class,
                AvroKeyOutputFormat.class, job.getConfiguration());
    }
}
