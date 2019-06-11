package spark.wiki.linked.nlp;

import avroschema.linked.SentenceLinked;
import avroschema.linked.WikiArticleLinked;
import avroschema.linked.WikiArticleLinkedNLP;
import avroschema.util.AvroUtils;
import avroschema.util.Utils;

import de.uni_mannheim.utils.coreNLP.NLPPipeline;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import nlp.WikiArticleTextDocument;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.List;

/**
 * @author Kiril Gashteovski
 */

public class WikiArticleAnnotateNLPLinked {
    public static final StanfordCoreNLP pipeline = NLPPipeline.StanfordDepNNParser();
    private static JavaSparkContext context;

    public static void main(String args[]) {
        // Initializations
        String articleReadDir = args[0];
        String articleWriteDir = args[1];
        SparkConf conf = new SparkConf().setAppName("WikiArticleAnnotateNLPLinked").remove("spark.serializer");
        context = new JavaSparkContext(conf);

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(WikiArticleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<WikiArticleLinked>, NullWritable> triplesPairRDD = (JavaPairRDD<AvroKey<WikiArticleLinked>, NullWritable>)
                context.newAPIHadoopFile(articleReadDir, AvroKeyInputFormat.class, WikiArticleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        // Annotate with NLP
        JavaRDD<WikiArticleLinkedNLP> wikiArticleNLPRDD = triplesPairRDD.map(tuple -> {
            WikiArticleLinked article = AvroUtils.cloneAvroRecord(tuple._1().datum());

            if (article.getText() == null) {
                return null;
            }

            // Annotate the sentences
            WikiArticleTextDocument doc = new WikiArticleTextDocument(article.getText().toString(), pipeline);
            doc.annotateSentences();
            List<SentenceLinked> sentences = doc.getSentences();
            if (sentences == null || sentences.isEmpty()) {
                return null;
            }

            // Put link information within all the tokens (if found)
            List<SentenceLinked> linkedSentences;
            if (article.getLinks() == null || article.getLinks().isEmpty()) {
                linkedSentences = sentences;
            } else {
                linkedSentences = Utils.getSentencesWithLinkedTokens(sentences, article.getLinks());
            }

            // Open WikiArticleNLP object
            WikiArticleLinkedNLP articleNLP = new WikiArticleLinkedNLP();
            articleNLP.setId(article.getId());
            articleNLP.setTitle(article.getTitle());
            articleNLP.setUrl(article.getUrl());
            articleNLP.setText(article.getText());
            articleNLP.setLinks(article.getLinks());
            articleNLP.setSentencesLinked(linkedSentences);

            return articleNLP;
        }).filter(a -> a != null);

        // Serializing to AVRO
        JavaPairRDD<AvroKey<WikiArticleLinkedNLP>, NullWritable> javaPairRDD = wikiArticleNLPRDD.mapToPair(r -> new Tuple2<>(new AvroKey<>(r), NullWritable.get()));
        Job job = AvroUtils.getJobOutputKeyAvroSchema(WikiArticleLinkedNLP.getClassSchema());
        javaPairRDD.saveAsNewAPIHadoopFile(articleWriteDir, AvroKey.class, NullWritable.class, AvroKeyOutputFormat.class,
                job.getConfiguration());

        context.close();
    }
}
