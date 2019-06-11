package spark.wiki.linked.triple.annotate;

import avroschema.linked.*;
import avroschema.triple.WikiTripleLinked;
import avroschema.util.AvroUtils;
import avroschema.util.Utils;
import de.uni_mannheim.minie.MinIE;
import de.uni_mannheim.minie.annotation.AnnotatedProposition;
import de.uni_mannheim.minie.annotation.SpaTe.space.Space;
import de.uni_mannheim.minie.annotation.SpaTe.space.SpaceUtils;
import de.uni_mannheim.minie.annotation.SpaTe.time.Time;
import de.uni_mannheim.minie.annotation.SpaTe.time.TimeUtils;
import de.uni_mannheim.utils.RedirectLinksMap;
import de.uni_mannheim.utils.coreNLP.NLPPipeline;
import edu.stanford.nlp.pipeline.AnnotationPipeline;
import edu.stanford.nlp.semgraph.SemanticGraph;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;

public class OPIEC {
    public static final AnnotationPipeline temporalPipeline = NLPPipeline.initTimePipeline();
    private static JavaSparkContext context;

    public static void main(String args[]) throws IOException {
        // Initializations
        String inputDirectory = args[0];
        String outputDirectory = args[1];

        // Initializations
        SparkConf conf = new SparkConf().setAppName("Triples").remove("spark.serializer");
        context = new JavaSparkContext(conf);
        Schema schema = AvroUtils.toSchema(WikiArticleLinkedNLP.class.getName());
        Job inputJob = AvroUtils.getJobInputKeyAvroSchema(schema);

        // Load dictionary of redirection links
        RedirectLinksMap REDIRECT_LINKS_MAP = new RedirectLinksMap("/redirects/redirects.dict");
        Broadcast<HashMap<String, String>> REDIRECT_LINKS_MAP_BROADCAST = context.broadcast(REDIRECT_LINKS_MAP.getRedirectMap());

        // Read from avro
        JavaPairRDD<AvroKey<WikiArticleLinkedNLP>, NullWritable> inputRecords = (JavaPairRDD<AvroKey<WikiArticleLinkedNLP>, NullWritable>)
                context.newAPIHadoopFile(inputDirectory, AvroKeyInputFormat.class, WikiArticleLinkedNLP.class, NullWritable.class, inputJob.getConfiguration());

        // Repartitioning
        JavaRDD<WikiArticleLinkedNLP> articlesRepartitionedRDD = inputRecords.map(tuple -> AvroUtils.cloneAvroRecord(tuple._1().datum())).repartition(1500);

        JavaRDD<TripleLinked> triplesRDD = articlesRepartitionedRDD.flatMap(article -> {
            //WikiArticleLinkedNLP article = AvroUtils.cloneAvroRecord(tuple._1().datum());
            ObjectArrayList<TripleLinked> triples = new ObjectArrayList<>();
            MinIE minie = new MinIE();
            String reftime = "XXXX-XX-XX";

            // HashMap: key = index of the token; value = WikiLink of the token
            HashMap<Integer, WikiLink> tokenLinks = new HashMap<>();

            // For each sentence extract triples and add them to the list
            for (SentenceLinked s : article.getSentencesLinked()) {
                // Dirty hack (this particular sentence runs forever on MinIE)
                if (article.getId().equals("849263")) {
                    if (s.getSId() == 283) {
                        continue;
                    }
                }

                if (s.getTokens().size() > 100) continue;

                // Initialize necessary objects
                for (TokenLinked t : s.getTokens()) {
                    tokenLinks.put(t.getIndex(), t.getWLink());
                }

                // Initialize necessary objects
                SemanticGraph sg = Utils.sentenceToSemanticGraph(s);
                if (sg == null) {
                    continue;
                }

                String sentence = Utils.sentenceLinkedToString(s);
                ObjectArrayList<Time> tempAnnotations = TimeUtils.annotateTime(sentence, temporalPipeline, reftime);
                ObjectArrayList<Space> spaceAnnotations = SpaceUtils.annotateSpace(sg);

                minie.clear();
                minie.setTempAnnotations(tempAnnotations);
                minie.setSpaceAnnotations(spaceAnnotations);
                minie.minimize(sg, MinIE.Mode.SAFE, null, true);
                minie.pruneAnnotatedPropositions();

                int sentenceTripleCounter = 0;
                for (AnnotatedProposition aProp : minie.getPropositions()) {
                    if (aProp.getRelation().getWordList().isEmpty()) {
                        continue;
                    }
                    sentenceTripleCounter++;
                    // Set the triple
                    WikiTripleLinked wikiTripleLinked = new WikiTripleLinked(aProp, s, article, tokenLinks);
                    String tripleID = "Wiki_" + article.getId() + "_" + s.getSId() + "_" + Integer.toString(sentenceTripleCounter);
                    wikiTripleLinked.populateTriple(tripleID);
                    wikiTripleLinked.populateCanonicalLinksMap(REDIRECT_LINKS_MAP_BROADCAST.getValue());
                    triples.add(wikiTripleLinked.getTripleLinked());
                }
            }

            return triples.iterator();
        });

        // Serializing to AVRO
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> tripleWritableRDD = triplesRDD.mapToPair(r -> new Tuple2<>(new AvroKey<>(r), NullWritable.get()));
        Job outputJob = AvroUtils.getJobOutputKeyAvroSchema(TripleLinked.getClassSchema());
        tripleWritableRDD.saveAsNewAPIHadoopFile(outputDirectory, AvroKey.class, NullWritable.class, AvroKeyOutputFormat.class,
                outputJob.getConfiguration());

        context.close();
    }
}
