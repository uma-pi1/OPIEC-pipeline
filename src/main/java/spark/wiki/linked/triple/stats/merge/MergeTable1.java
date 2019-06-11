package spark.wiki.linked.triple.stats.merge;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author Kiril Gashteovski
 */

public class MergeTable1 {
    private static JavaSparkContext context;

    public static void main(String args[]) {
        String TABLE_1_DIR = args[0];
        SparkConf conf = new SparkConf().setAppName("Table1").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Paths to be merged
        String AttributionTriplesCount = TABLE_1_DIR + "/AttributionTriplesCount/*";
        String NegTriplesCount = TABLE_1_DIR + "/NegTriplesCount/*";
        String ObjLength = TABLE_1_DIR + "/ObjLength/*";
        String PossTriplesCount = TABLE_1_DIR + "/PossTriplesCount/*";
        String QuantityTriplesCount = TABLE_1_DIR + "/QuantityTriplesCount/*";
        String RelLength = TABLE_1_DIR + "/RelLength/*";
        String SPOTLTriplesCount = TABLE_1_DIR + "/SPOTLTriplesCount/*";
        String SemAnnotationCount = TABLE_1_DIR + "/SemAnnotationCount/*";
        String SpaceOrTimeTriplesCount = TABLE_1_DIR + "/SpaceOrTimeTriplesCount/*";
        String SpatialTriplesCount = TABLE_1_DIR + "/SpatialTriplesCount/*";
        String SubjLength = TABLE_1_DIR + "/SubjLength/*";
        String TemporalTriplesCount = TABLE_1_DIR + "/TemporalTriplesCount/*";
        String TriplesCount = TABLE_1_DIR + "/TriplesCount/*";
        String TriplesLength = TABLE_1_DIR + "/TriplesLength/*";
        String TriplesWithDTCount = TABLE_1_DIR  + "/TriplesWithDTCount/*";
        String TriplesWithPRPCount = TABLE_1_DIR + "/TriplesWithPRPCount/*";
        String TriplesWithSameLinkForArgs = TABLE_1_DIR + "/TriplesWithSameLinkForArgs/*";
        String TriplesWithWPCount = TABLE_1_DIR + "/TriplesWithWPCount/*";

        // Merge
        context.textFile(AttributionTriplesCount).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/AttributionTriplesCount-MERGED");
        context.textFile(NegTriplesCount).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/NegTriplesCount-MERGED");
        context.textFile(ObjLength).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/ObjLength-MERGED");
        context.textFile(PossTriplesCount).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/PossTriplesCount-MERGED");
        context.textFile(QuantityTriplesCount).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/QuantityTriplesCount-MERGED");
        context.textFile(RelLength).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/RelLength-MERGED");
        context.textFile(SPOTLTriplesCount).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/SPOTLTriplesCount-MERGED");
        context.textFile(SemAnnotationCount).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/SemAnnotationCount-MERGED");
        context.textFile(SpaceOrTimeTriplesCount).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/SpaceOrTimeTriplesCount-MERGED");
        context.textFile(SpatialTriplesCount).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/SpatialTriplesCount-MERGED");
        context.textFile(SubjLength).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/SubjLength-MERGED");
        context.textFile(TemporalTriplesCount).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/TemporalTriplesCount-MERGED");
        context.textFile(TriplesCount).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/TriplesCount-MERGED");
        context.textFile(TriplesLength).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/TriplesLength-MERGED");
        context.textFile(TriplesWithDTCount).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/TriplesWithDTCount-MERGED");
        context.textFile(TriplesWithPRPCount).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/TriplesWithPRPCount-MERGED");
        context.textFile(TriplesWithSameLinkForArgs).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/TriplesWithSameLinkForArgs-MERGED");
        context.textFile(TriplesWithWPCount).coalesce(1).saveAsTextFile(TABLE_1_DIR + "/TriplesWithWPCount-MERGED");
    }
}
