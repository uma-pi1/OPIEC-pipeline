package spark.wiki.linked.triple.stats.count.arguments;

import avroschema.linked.TripleLinked;
import avroschema.util.AvroUtils;
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
import org.apache.spark.api.java.JavaDoubleRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Kiril Gashteovski
 *
 * Given a corpus of linked triples, count the number of distinct arguments.
 * NOTE: used to count unique arguments in Table 1 in the paper
 */

public class CountDistinctArgs {
    private static final Logger logger = LogManager.getLogger("CountDistinctArgs");
    private static JavaSparkContext context;

    public static void main(String args[]) {
        logger.info("Begin spark application: CountDistinctArgs");

        // Initializations
        String WL_DIR = args[0];
        String WRITE_DIR = args[1];
        SparkConf conf = new SparkConf().setAppName("WikiTriplesLinked").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> wikiTriplesPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(WL_DIR, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        // Useless arguments (excluding triples having the same link for subj/obj)
        JavaPairRDD<String, Integer> argsRDD = wikiTriplesPairRDD.flatMapToPair((Tuple2<AvroKey<TripleLinked>, NullWritable> tuple) -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            List<Tuple2<String, Integer>> arguments = new ArrayList<>();
            arguments.add(new Tuple2<>(TripleLinkedUtils.subjToLemmatizedString(triple).toLowerCase(), 1));
            arguments.add(new Tuple2<>(TripleLinkedUtils.objToLemmatizedString(triple).toLowerCase(), 1));
            return arguments.iterator();
        }).reduceByKey((a, b) -> a+b);

        // Write the counts to HDFS
        double count = argsRDD.count();
        List<Double> countAsList = new ArrayList<>();
        countAsList.add(count);
        JavaDoubleRDD countAsListRDD = context.parallelizeDoubles(countAsList).coalesce(1);
        countAsListRDD.saveAsTextFile(WRITE_DIR);
    }
}
