package spark.wiki.linked.nlp;

import avroschema.linked.SentenceLinked;
import avroschema.linked.WikiArticleLinkedNLP;
import avroschema.util.AvroUtils;
import avroschema.util.SentenceLinkedUtil;

import de.uni_mannheim.utils.coreNLP.NLPPipeline;

import edu.stanford.nlp.pipeline.AnnotationPipeline;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

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

public class SentencesSpaceTimeStats {
    private static JavaSparkContext sc;
    public static final AnnotationPipeline temporalPipeline = NLPPipeline.initTimePipeline();

    public static void main(String args[]) {
        // Input parameters
        String inputDirectory = args[0];
        String timeDir = args[1];
        String spaceDir = args[2];
        String spaceOrTimeDir = args[3];

        // Initializations
        SparkConf conf = new SparkConf().setAppName("SpaceTimeStats").remove("spark.serializer");
        sc = new JavaSparkContext(conf);
        Schema schema = AvroUtils.toSchema(WikiArticleLinkedNLP.class.getName());
        Job job = AvroUtils.getJobInputKeyAvroSchema(schema);

        // Article RDDs
        JavaPairRDD<AvroKey<WikiArticleLinkedNLP>, NullWritable> inputRecords = (JavaPairRDD<AvroKey<WikiArticleLinkedNLP>, NullWritable>)
                sc.newAPIHadoopFile(inputDirectory, AvroKeyInputFormat.class,
                        WikiArticleLinkedNLP.class, NullWritable.class, job.getConfiguration());

        // For each article, aggregate the counts of sentences containing and not containing temporal annotations
        JavaPairRDD<String, Integer> timeCountsRDD = inputRecords.flatMapToPair((Tuple2<AvroKey<WikiArticleLinkedNLP>, NullWritable> tuple) -> {
            // Hadoop's RecordReader reuses the same Writable object for all records which may lead to undesired behavior
            // when caching RDD. Cloning records solves this problem.
            WikiArticleLinkedNLP article = AvroUtils.cloneAvroRecord(tuple._1.datum());
            ObjectArrayList<Tuple2<String, Integer>> tempCount = new ObjectArrayList<>();

            for (SentenceLinked s: article.getSentencesLinked()){
                if (SentenceLinkedUtil.hasTime(s, temporalPipeline, "XXXX-XX-XX")) {
                    tempCount.add(new Tuple2<>("HAS_TIME", 1));
                } else {
                    tempCount.add(new Tuple2<>("HAS_NO_TIME", 1));
                }
            }

            return tempCount.iterator();
        }).reduceByKey((a, b) -> a + b)
                .coalesce(1);

        // For each article, aggregate the counts of sentences containing and not containing spatial annotations
        JavaPairRDD<String, Integer> spaceCountsRDD = inputRecords.flatMapToPair((Tuple2<AvroKey<WikiArticleLinkedNLP>, NullWritable> tuple) -> {
            // Hadoop's RecordReader reuses the same Writable object for all records which may lead to undesired behavior
            // when caching RDD. Cloning records solves this problem.
            WikiArticleLinkedNLP article = AvroUtils.cloneAvroRecord(tuple._1.datum());
            ObjectArrayList<Tuple2<String, Integer>> spaceCount = new ObjectArrayList<>();

            for (SentenceLinked s: article.getSentencesLinked()) {
                if (SentenceLinkedUtil.hasLocation(s)) {
                    spaceCount.add(new Tuple2<>("HAS_SPACE", 1));
                } else {
                    spaceCount.add(new Tuple2<>("HAS_NO_SPACE", 1));
                }
            }

            return spaceCount.iterator();
        }).reduceByKey((a, b) -> a + b)
                .coalesce(1);

        // For each article, aggregate the counts of sentences containing and not containing either spatial or temporal annotations
        JavaPairRDD<String, Integer> spaceOrTimeCountsRDD = inputRecords.flatMapToPair((Tuple2<AvroKey<WikiArticleLinkedNLP>, NullWritable> tuple) -> {
            // Hadoop's RecordReader reuses the same Writable object for all records which may lead to undesired behavior
            // when caching RDD. Cloning records solves this problem.
            WikiArticleLinkedNLP article = AvroUtils.cloneAvroRecord(tuple._1.datum());
            ObjectArrayList<Tuple2<String, Integer>> spaceCount = new ObjectArrayList<>();

            for (SentenceLinked s: article.getSentencesLinked()) {
                if (SentenceLinkedUtil.hasLocation(s) || SentenceLinkedUtil.hasTime(s, temporalPipeline, "XXXX-XX-XX")) {
                    spaceCount.add(new Tuple2<>("HAS_SPACE_OR_TIME", 1));
                } else {
                    spaceCount.add(new Tuple2<>("HAS_NO_SPACE_OR_TIME", 1));
                }
            }

            return spaceCount.iterator();
        }).reduceByKey((a, b) -> a + b)
                .coalesce(1);

        // Write the results
        timeCountsRDD.saveAsTextFile(timeDir);
        spaceCountsRDD.saveAsTextFile(spaceDir);
        spaceOrTimeCountsRDD.saveAsTextFile(spaceOrTimeDir);
    }
}
