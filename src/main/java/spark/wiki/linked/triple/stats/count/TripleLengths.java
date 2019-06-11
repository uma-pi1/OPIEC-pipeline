package spark.wiki.linked.triple.stats.count;

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

public class TripleLengths {
    private static JavaSparkContext context;

    public static void main(String args[]) {
        // Initializations
        String linkedTriplesReadDir = args[0];
        String linkedTriplesWriteDir = args[1];
        SparkConf conf = new SparkConf().setAppName("Table1").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> wikiArticlesNLPPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(linkedTriplesReadDir, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        /** Triples' length **/
        JavaPairRDD<Integer, Integer> tripleLengthRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            return new Tuple2<>(triple.getSubject().size() + triple.getRelation().size() + triple.getObject().size(), 1);
        }).reduceByKey((a, b) -> (a + b));
        JavaPairRDD<Integer, Integer> subjectLengthRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            return new Tuple2<>(triple.getSubject().size(), 1);
        }).reduceByKey((a, b) -> (a + b));
        JavaPairRDD<Integer, Integer> relationLengthRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            return new Tuple2<>(triple.getRelation().size(), 1);
        }).reduceByKey((a, b) -> (a + b));
        JavaPairRDD<Integer, Integer> objectLengthRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            return new Tuple2<>(triple.getObject().size(), 1);
        }).reduceByKey((a, b) -> (a + b));


        tripleLengthRDD.saveAsTextFile(linkedTriplesWriteDir + "/TriplesLength");
        subjectLengthRDD.saveAsTextFile(linkedTriplesWriteDir + "/SubjLength");
        relationLengthRDD.saveAsTextFile(linkedTriplesWriteDir + "/RelLength");
        objectLengthRDD.saveAsTextFile(linkedTriplesWriteDir + "/ObjLength");
    }
}
