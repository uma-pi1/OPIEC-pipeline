package spark.wiki.linked.triple.stats.count;

import avroschema.linked.TripleLinked;
import avroschema.util.AvroUtils;
import avroschema.util.TokenLinkedUtils;
import de.uni_mannheim.constant.CHARACTER;
import de.uni_mannheim.constant.SEPARATOR;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Kiril Gashteovski
 */

public class Table2 {
    private static JavaSparkContext context;

    public static void main(String args[]) {
        // Initializations
        String linkedTriplesReadDir = args[0];
        String linkedTriplesWriteDir = args[1];
        SparkConf conf = new SparkConf().setAppName("Table2").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> wikiArticlesNLPPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(linkedTriplesReadDir, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        /** Distinct facts/subj/rel/obj **/
        JavaPairRDD<String, Integer> spoCount = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            StringBuilder sb = new StringBuilder();
            sb.append(CHARACTER.QUOTATION_MARK);
            sb.append(TokenLinkedUtils.linkedTokensLemmasToString(triple.getSubject()).toLowerCase());
            sb.append(CHARACTER.QUOTATION_MARK);
            sb.append(SEPARATOR.TAB);
            sb.append(CHARACTER.QUOTATION_MARK);
            sb.append(TokenLinkedUtils.linkedTokensLemmasToString(triple.getRelation()).toLowerCase());
            sb.append(CHARACTER.QUOTATION_MARK);
            sb.append(SEPARATOR.TAB);
            sb.append(CHARACTER.QUOTATION_MARK);
            sb.append(TokenLinkedUtils.linkedTokensLemmasToString(triple.getObject()).toLowerCase());
            sb.append(CHARACTER.QUOTATION_MARK);
            return new Tuple2<>(sb.toString().trim(), 1);
        }).reduceByKey((a, b) -> (a + b));
        JavaPairRDD<String, Integer> argCount = wikiArticlesNLPPairRDD.flatMapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            List<Tuple2<String, Integer>> argCounts = new ArrayList<>();
            argCounts.add(new Tuple2<>(TokenLinkedUtils.linkedTokensLemmasToString(triple.getSubject()).toLowerCase(), 1));
            argCounts.add(new Tuple2<>(TokenLinkedUtils.linkedTokensLemmasToString(triple.getObject()).toLowerCase(), 1));
            return argCounts.iterator();
        }).reduceByKey((a, b) -> (a + b));
        JavaPairRDD<String, Integer> relCount = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            return new Tuple2<>(TokenLinkedUtils.linkedTokensLemmasToString(triple.getRelation()).toLowerCase(), 1);
        }).reduceByKey((a, b) -> (a + b));

        /** Write to directories**/
        spoCount.saveAsTextFile(linkedTriplesWriteDir + "/SPOCounts");
        argCount.saveAsTextFile(linkedTriplesWriteDir + "/ArgCounts");
        relCount.saveAsTextFile(linkedTriplesWriteDir + "/RelCounts");
    }
}
