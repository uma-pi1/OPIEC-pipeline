package spark.wiki.linked.triple.stats.count;

import avroschema.linked.TripleLinked;
import avroschema.util.AvroUtils;
import avroschema.util.TokenLinkedUtils;
import avroschema.util.TripleLinkedUtils;
import avroschema.util.Utils;
import de.uni_mannheim.minie.annotation.factuality.Modality;
import de.uni_mannheim.minie.annotation.factuality.Polarity;
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

public class Table1 {
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

        /** Number of triples **/
        JavaPairRDD<Integer, Integer> triplesCountRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            return new Tuple2<>(1, 1);
        }).reduceByKey((a, b) -> (a + b));
        JavaPairRDD<String, Integer> tripleWithPRPCountRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            if (TokenLinkedUtils.isPRP(triple.getSubject()) || TokenLinkedUtils.isPRP(triple.getObject())) {
                return new Tuple2<>("WITH_PRP", 1);
            } else {
                return new Tuple2<>("WITHOUT_PRP", 1);
            }
        }).reduceByKey((a, b) -> (a + b));
        JavaPairRDD<String, Integer> triplesWithSameLinkForArguments = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            if (TripleLinkedUtils.argumentsAreSameLink(triple)) {
                return new Tuple2<>("WITH_SAME_LINK", 1);
            } else {
                return new Tuple2<>("WITHOUT_SAME_LINK", 1);
            }
        }).reduceByKey((a, b) -> (a + b));

        /** Triple length **/
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

        /** Count semantic annotations **/
        JavaPairRDD<String, Integer> semanticAnnotationCountRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            if (Utils.hasSemanticAnnotation(triple)) {
                return new Tuple2<>("WITH_SEM_ANNOTATION", 1);
            } else {
                return new Tuple2<>("WITHOUT_SEM_ANNOTATION",1);
            }
        }).reduceByKey((a, b) -> (a + b));
        JavaPairRDD<String, Integer> negTriplesCountRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            if (triple.getPolarity().equals(Polarity.ST_NEGATIVE)) {
                return new Tuple2<>("-", 1);
            } else {
                return new Tuple2<>("+",1);
            }
        }).reduceByKey((a, b) -> (a + b));
        JavaPairRDD<String, Integer> possTriplesCountRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            if (triple.getModality().equals(Modality.ST_POSSIBILITY)) {
                return new Tuple2<>("PS", 1);
            } else {
                return new Tuple2<>("CT",1);
            }
        }).reduceByKey((a, b) -> (a + b));
        JavaPairRDD<String, Integer> attributionTriplesCountRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            if (triple.getAttributionLinked() != null) {
                return new Tuple2<>("WITH_ATTRIBUTION", 1);
            } else {
                return new Tuple2<>("WITHOUT_ATTRIBUTION",1);
            }
        }).reduceByKey((a, b) -> (a + b));
        JavaPairRDD<String, Integer> quantityTriplesCountRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            if (Utils.hasQuantity(triple)) {
                return new Tuple2<>("HAS_QUANTITY", 1);
            } else {
                return new Tuple2<>("HAS_NO_QUANTITY", 1);
            }
        }).reduceByKey((a, b) -> (a + b));
        JavaPairRDD<String, Integer> temporalTriplesCountRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            if (Utils.hasTemporalAnnotation(triple)) {
                return new Tuple2<>("HAS_TIME", 1);
            } else {
                return new Tuple2<>("HAS_NO_TIME",1);
            }
        }).reduceByKey((a, b) -> (a + b));
        JavaPairRDD<String, Integer> spatialTriplesCountRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            if (Utils.hasSpatialAnnotation(triple)) {
                return new Tuple2<>("HAS_SPACE", 1);
            } else {
                return new Tuple2<>("HAS_NO_SPACE", 1);
            }
        }).reduceByKey((a, b) -> (a + b));
        JavaPairRDD<String, Integer> SPOTLTriplesCountRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            if (Utils.hasTemporalAnnotation(triple) && Utils.hasSpatialAnnotation(triple)) {
                return new Tuple2<>("HAS_SPOTL", 1);
            } else {
                return new Tuple2<>("HAS_NO_SPOTL", 1);
            }
        }).reduceByKey((a, b) -> (a + b));
        JavaPairRDD<String, Integer> spaceOrTimeTripleCountRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            if (Utils.hasTemporalAnnotation(triple) || Utils.hasSpatialAnnotation(triple)) {
                return new Tuple2<>("HAS_SPACE_OR_TIME", 1);
            } else {
                return new Tuple2<>("HAS_NO_SPACE_NOR_TIME", 1);
            }
        }).reduceByKey((a, b) -> (a + b));

        JavaPairRDD<String, Integer> tripleWithDTCountRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            if (TokenLinkedUtils.isDT(triple.getSubject()) || TokenLinkedUtils.isDT(triple.getObject())) {
                return new Tuple2<>("WITH_DT", 1);
            } else {
                return new Tuple2<>("WITHOUT_DT", 1);
            }
        }).reduceByKey((a, b) -> (a + b));

        JavaPairRDD<String, Integer> tripleWithWPCountRDD = wikiArticlesNLPPairRDD.mapToPair(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());
            if (TokenLinkedUtils.isWP(triple.getSubject()) || TokenLinkedUtils.isWP(triple.getObject())) {
                return new Tuple2<>("WITH_WP", 1);
            } else {
                return new Tuple2<>("WITHOUT_WP", 1);
            }
        }).reduceByKey((a, b) -> (a + b));

        /** Write results **/
        triplesCountRDD.saveAsTextFile(linkedTriplesWriteDir + "/TriplesCount");
        tripleWithPRPCountRDD.saveAsTextFile(linkedTriplesWriteDir + "/TriplesWithPRPCount");
        tripleWithDTCountRDD.saveAsTextFile(linkedTriplesWriteDir + "/TriplesWithDTCount");
        tripleWithWPCountRDD.saveAsTextFile(linkedTriplesWriteDir + "/TriplesWithWPCount");
        triplesWithSameLinkForArguments.saveAsTextFile(linkedTriplesWriteDir + "/TriplesWithSameLinkForArgs");
        tripleLengthRDD.saveAsTextFile(linkedTriplesWriteDir + "/TriplesLength");
        subjectLengthRDD.saveAsTextFile(linkedTriplesWriteDir + "/SubjLength");
        relationLengthRDD.saveAsTextFile(linkedTriplesWriteDir + "/RelLength");
        objectLengthRDD.saveAsTextFile(linkedTriplesWriteDir + "/ObjLength");
        semanticAnnotationCountRDD.saveAsTextFile(linkedTriplesWriteDir + "/SemAnnotationCount");
        negTriplesCountRDD.saveAsTextFile(linkedTriplesWriteDir + "/NegTriplesCount");
        possTriplesCountRDD.saveAsTextFile(linkedTriplesWriteDir + "/PossTriplesCount");
        temporalTriplesCountRDD.saveAsTextFile(linkedTriplesWriteDir + "/TemporalTriplesCount");
        spatialTriplesCountRDD.saveAsTextFile(linkedTriplesWriteDir + "/SpatialTriplesCount");
        SPOTLTriplesCountRDD.saveAsTextFile(linkedTriplesWriteDir + "/SPOTLTriplesCount");
        spaceOrTimeTripleCountRDD.saveAsTextFile(linkedTriplesWriteDir + "/SpaceOrTimeTriplesCount");
        attributionTriplesCountRDD.saveAsTextFile(linkedTriplesWriteDir + "/AttributionTriplesCount");
        quantityTriplesCountRDD.saveAsTextFile(linkedTriplesWriteDir + "/QuantityTriplesCount");
    }
}
