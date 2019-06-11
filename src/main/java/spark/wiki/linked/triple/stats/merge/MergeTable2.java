package spark.wiki.linked.triple.stats.merge;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author Kiril Gashteovski
 */

public class MergeTable2 {
    private static JavaSparkContext context;

    public static void main(String args[]) {
        String TABLE_2_DIR = args[0];
        SparkConf conf = new SparkConf().setAppName("Table2").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        String ArgCounts = TABLE_2_DIR + "/ArgCounts/*";
        String RelCounts = TABLE_2_DIR + "/RelCounts/*";
        String SPOCounts = TABLE_2_DIR + "/SPOCounts/*";

        context.textFile(ArgCounts).coalesce(1).saveAsTextFile(TABLE_2_DIR + "/ArgCounts-MERGED");
        context.textFile(RelCounts).coalesce(1).saveAsTextFile(TABLE_2_DIR + "/RelCounts-MERGED");
        context.textFile(SPOCounts).coalesce(1).saveAsTextFile(TABLE_2_DIR + "/SPOCounts-MERGED");
    }
}
