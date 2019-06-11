package spark.wiki.linked.triple.select;

import avroschema.linked.TripleLinked;
import avroschema.util.AvroUtils;
import avroschema.util.TokenLinkedUtils;
import avroschema.util.Utils;
import de.uni_mannheim.utils.Dictionary;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
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
/**
 * @author Kiril Gashteovski
 */

public class OpiecClean {
    /**
     * From all of the triples, select just the ones where both the subject and the object are linked arguments, NERs or concepts
     * NOTE: we use this script to generate OPIEC-Clean
     */
    private static JavaSparkContext context;

    public static void main(String args[]) throws IOException {
        // Initializations
        String OPIEC_DIR = args[0];
        String OPIEC_CLEAN_DIR = args[1];
        SparkConf conf = new SparkConf().setAppName("WikiTriplesLinked").remove("spark.serializer");
        context = new JavaSparkContext(conf);
        Dictionary DICT_CONCEPTS = new Dictionary("/concepts/enwiki-latest-all-titles-in-ns0_lowercased.dict");
        Broadcast<ObjectOpenHashSet<String>> DICT_CONCEPTS_BROADCAST = context.broadcast(DICT_CONCEPTS.words());

        // Read from avro schemas
        Schema triplesSchema = AvroUtils.toSchema(TripleLinked.class.getName());
        Job triplesInputJob = AvroUtils.getJobInputKeyAvroSchema(triplesSchema);
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> wikiArticlesNLPPairRDD = (JavaPairRDD<AvroKey<TripleLinked>, NullWritable>)
                context.newAPIHadoopFile(OPIEC_DIR, AvroKeyInputFormat.class, TripleLinked.class, NullWritable.class, triplesInputJob.getConfiguration());

        JavaRDD<TripleLinked> triplesRDD = wikiArticlesNLPPairRDD.map(tuple -> {
            TripleLinked triple = AvroUtils.cloneAvroRecord(tuple._1().datum());

            if (triple.getObject().size() == 0 || triple.getObject() == null) {
                return null;
            }

            boolean subjIsOneLink = TokenLinkedUtils.allTokensAreOneLink(triple.getSubject());
            boolean subjIsQuantityWithOneLink = TokenLinkedUtils.allTokensAreQuantityWithOneLink(triple.getSubject());
            boolean subjIsOneNER = TokenLinkedUtils.allTokensAreOneNER(triple.getSubject());
            boolean subjIsQuantityWithOneNER = TokenLinkedUtils.allTokensAreQuantityWithOneNER(triple.getSubject());
            boolean subjIsConcept = Utils.isInDict(triple.getSubject(), DICT_CONCEPTS_BROADCAST);
            boolean subjIsConceptWithQuantity = Utils.isInDictWithQuantity(triple.getSubject(), DICT_CONCEPTS_BROADCAST);

            boolean objIsOneLink = TokenLinkedUtils.allTokensAreOneLink(triple.getObject());
            boolean objIsQuantityWithOneLink = TokenLinkedUtils.allTokensAreQuantityWithOneLink(triple.getObject());
            boolean objIsOneNER = TokenLinkedUtils.allTokensAreOneNER(triple.getObject());
            boolean objIsQuantityWithOneNER = TokenLinkedUtils.allTokensAreQuantityWithOneNER(triple.getObject());
            boolean objIsConcept = Utils.isInDict(triple.getObject(), DICT_CONCEPTS_BROADCAST);
            boolean objIsConceptWithQuantity = Utils.isInDictWithQuantity(triple.getObject(), DICT_CONCEPTS_BROADCAST);

            boolean subjPositiveCondition = subjIsOneLink || subjIsQuantityWithOneLink || subjIsOneNER || subjIsQuantityWithOneNER ||
                                            subjIsConcept || subjIsConceptWithQuantity;
            boolean objPositiveCondition = objIsOneLink || objIsQuantityWithOneLink || objIsOneNER || objIsQuantityWithOneNER ||
                                            objIsConcept || objIsConceptWithQuantity;

            // Check if the triple is underspecific
            if (TokenLinkedUtils.isPRP(triple.getSubject()) || TokenLinkedUtils.isPRP(triple.getObject())) {
                return null;
            }
            if (TokenLinkedUtils.isDT(triple.getSubject()) || TokenLinkedUtils.isDT(triple.getObject())) {
                return null;
            }
            if (TokenLinkedUtils.isWP(triple.getSubject()) || TokenLinkedUtils.isWP(triple.getObject())) {
                return null;
            }

            if (subjPositiveCondition && objPositiveCondition) {

                // Check if the triple is a broken-up entity
                boolean subjLinkFlag = subjIsOneLink || subjIsQuantityWithOneLink;
                boolean objLinkFlag = objIsOneLink || objIsQuantityWithOneLink;
                if (subjLinkFlag && objLinkFlag) {
                    // Check if the triple has same link
                    String subjLink = triple.getSubject().get(0).getWLink().getWikiLink();
                    if (subjLink.equals("")) {
                        subjLink = triple.getSubject().get(triple.getSubject().size() - 1).getWLink().getWikiLink();
                    }

                    String objLink = triple.getObject().get(0).getWLink().getWikiLink();
                    if (objLink.equals("")) {
                        objLink = triple.getObject().get(triple.getObject().size() - 1).getWLink().getWikiLink();
                    }

                    if (subjLink.equals(objLink)) {
                        return null;
                    }
                }

                return triple;
            }

            return null;
        }).filter(a -> a != null);

        // Serializing to AVRO
        JavaPairRDD<AvroKey<TripleLinked>, NullWritable> javaPairRDD = triplesRDD.mapToPair(r -> new Tuple2<>(new AvroKey<>(r), NullWritable.get()));
        Job job = AvroUtils.getJobOutputKeyAvroSchema(TripleLinked.getClassSchema());
        javaPairRDD.saveAsNewAPIHadoopFile(OPIEC_CLEAN_DIR, AvroKey.class, NullWritable.class,
                AvroKeyOutputFormat.class, job.getConfiguration());
    }
}
