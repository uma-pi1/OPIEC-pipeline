package spark.wiki.linked.triple.stats.count.triple;

import avroschema.linked.TripleLinked;
import avroschema.util.AvroUtils;
import de.uni_mannheim.constant.POS_TAG;
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
 * Count SV triples which do not have PRP or DT for subj
 */
public class CountSVTriples {
    private static JavaSparkContext context;

    public static void main(String args[]) {
        // Initializations
        String OPIEC_DIR = args[0];
        String WRITE_DIR = args[1];
        SparkConf conf = new SparkConf().setAppName("WikiTriplesLinked").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> wikiTriplesPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(OPIEC_DIR, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        // Useless arguments (excluding triples having the same link for subj/obj)
        JavaPairRDD<String, Integer> SVTriplesRDD = wikiTriplesPairRDD.mapToPair((Tuple2<AvroKey<TripleLinked>, NullWritable> tuple) -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());

            if (triple.getObject() == null || triple.getObject().isEmpty()) {
                if (triple.getSubject().size() == 1) {
                    if (triple.getSubject().get(0).getPos().contains(POS_TAG.PRP)) {
                        return new Tuple2<>("NO_SV",1);
                    }
                    else if (triple.getSubject().get(0).getPos().equals(POS_TAG.DT)) {
                        return new Tuple2<>("NO_SV", 1);
                    } else {
                        return new Tuple2<>("SV", 1);
                    }
                }

                return new Tuple2<>("SV", 1);
            }

            return new Tuple2<>("NO_SV", 1);
        }).reduceByKey((a, b) -> (a + b))
                .coalesce(1);

        SVTriplesRDD.saveAsTextFile(WRITE_DIR);
    }
}
