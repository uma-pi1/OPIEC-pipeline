package spark.wiki.linked.triple.stats.count.arguments;

import avroschema.linked.TripleLinked;
import avroschema.util.AvroUtils;
import avroschema.util.TokenLinkedUtils;
import avroschema.util.TripleLinkedUtils;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Kiril Gashteovski
 *
 * Given the "useful triples dataset", aggregate just the arguments which are not NERs
 */

public class CountNoNERArgs {
    private static final Logger logger = LogManager.getLogger("CountUselessArguments");
    private static JavaSparkContext context;

    public static void main(String args[]) throws IOException {
        logger.info("Begin spark application: CountUselessArguments");

        // Initializations
        String WLNCClean_DIR = args[0];
        String WRITE_DIR = args[1];
        SparkConf conf = new SparkConf().setAppName("WikiTriplesLinked").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> wikiTriplesPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(WLNCClean_DIR, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        // Useless arguments (excluding triples having the same link for subj/obj)
        JavaPairRDD<String, Integer> noNerArgsRDD = wikiTriplesPairRDD.flatMapToPair((Tuple2<AvroKey<TripleLinked>, NullWritable> tuple) -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());

            List<Tuple2<String, Integer>> noNerArguments = new ArrayList<>();

            boolean subjIsOneNER = TokenLinkedUtils.allTokensAreOneNER(triple.getSubject());
            boolean subjIsQuantityWithOneNER = TokenLinkedUtils.allTokensAreQuantityWithOneNER(triple.getSubject());
            if (!subjIsOneNER && !subjIsQuantityWithOneNER) {
                noNerArguments.add(new Tuple2<>(TripleLinkedUtils.subjToLemmatizedString(triple).toLowerCase(), 1));
            }

            boolean objIsOneNER = TokenLinkedUtils.allTokensAreOneNER(triple.getObject());
            boolean objIsQuantityWithOneNER = TokenLinkedUtils.allTokensAreQuantityWithOneNER(triple.getObject());
            if (!objIsOneNER && !objIsQuantityWithOneNER) {
                noNerArguments.add(new Tuple2<>(TripleLinkedUtils.objToLemmatizedString(triple).toLowerCase(), 1));
            }

            return noNerArguments.iterator();
        }).reduceByKey((a, b) -> (a + b))
                .filter(a -> a._2() >= 10)
                .coalesce(1);

        noNerArgsRDD.saveAsTextFile(WRITE_DIR);
    }
}
