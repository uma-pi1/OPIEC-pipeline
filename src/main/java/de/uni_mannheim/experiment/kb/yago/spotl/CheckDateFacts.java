package de.uni_mannheim.experiment.kb.yago.spotl;

import avroschema.linked.TokenLinked;
import avroschema.linked.TripleLinked;
import avroschema.util.SentenceLinkedUtil;
import avroschema.util.TripleLinkedUtils;

import de.uni_mannheim.kb.yago.YAGOCoreFacts;

import de.uni_mannheim.minie.annotation.SpaTe.time.Time;
import de.uni_mannheim.minie.annotation.SpaTe.time.TimeUtils;
import de.uni_mannheim.utils.RedirectLinksMap;
import de.uni_mannheim.utils.coreNLP.NLPPipeline;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.pipeline.AnnotationPipeline;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple4;
import scala.Tuple3;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author Kiril Gashteovski
 * @author Sven Hertling
 */

public class CheckDateFacts {
    private static final Logger logger = LoggerFactory.getLogger(CheckDateFacts.class);
    private static final String LINKED_DATE_FACTS_DIR = "experiments/SPOTL/OPIEC-Clean-LinkedDateFacts";

    public static void main(String args[]) throws IOException {
        AnnotationPipeline temporalPipeline = NLPPipeline.initTimePipeline();

        YAGOCoreFacts dateFacts = new YAGOCoreFacts();
        dateFacts.loadYagoDateFacts();

        // Load date facts into hashmap
        logger.info("Loading date facts into hash map ... ");
        HashMap<String, List<Tuple3<String, String, String>>> tempFacts = new HashMap<>();
        int yagoDateFactCount = 0;
        for (String key: dateFacts.getYagoDateFacts().keySet()) {
            yagoDateFactCount++;
            Tuple4<String, String, String, String> dateFact = dateFacts.getYagoDateFacts().get(key);
            if (tempFacts.containsKey(dateFact._1())) {
                tempFacts.get(dateFact._1()).add(new Tuple3<>(dateFact._2(), dateFact._3(), dateFact._4()));
            } else {
                List<Tuple3<String, String, String>> list = new ArrayList<>();
                list.add(new Tuple3<>(dateFact._2(), dateFact._3(), dateFact._4()));
                tempFacts.put(dateFact._1(), list);
            }
        }
        logger.info("Date facts loaded into hash map!");

        // OIE linked triples
        File folder = new File(LINKED_DATE_FACTS_DIR);
        File[] listOfFiles = folder.listFiles();
        String subjLink;
        String subjCanonicalLink;
        int oieDateFactCount = 0, oieDateFactHits = 0;
        int oieDateYearHitCount = 0, oieDateYearXXHitCount = 0;
        int oieDateMonthHitCount = 0, oieDateMonthXXHitCount = 0;
        int oieDateDayHitCount = 0;
        logger.info("Loading OIE triples ... ");
        for (int i = 0; i < listOfFiles.length; i++) {
            // Open avro file with triples
            System.out.println("Process File: " + listOfFiles[i].toString() + " File no: " + i);
            DatumReader<TripleLinked> userDatumReader = new SpecificDatumReader<>(TripleLinked.class);
            DataFileReader<TripleLinked> dataFileReader = new DataFileReader<>(listOfFiles[i], userDatumReader);
            TripleLinked triple;

            while (dataFileReader.hasNext()) {
                oieDateFactCount++;
                triple = dataFileReader.next();
                subjLink = TripleLinkedUtils.getSubjLink(triple);
                HashMap<String, String> canonicalLinksUnderscored = RedirectLinksMap.getLinksUnderscored(triple.getCanonicalLinks());
                if (canonicalLinksUnderscored.containsKey(subjLink)) {
                    subjCanonicalLink = canonicalLinksUnderscored.get(subjLink);
                } else {
                    subjCanonicalLink = subjLink;
                }

                if (tempFacts.containsKey(subjCanonicalLink)) {
                    oieDateFactHits++;
                    List<Tuple3<String, String, String>> yagoFacts = tempFacts.get(subjCanonicalLink);
                    String sentence = SentenceLinkedUtil.getSentenceLinkedString(triple.getSentenceLinked());
                    ObjectArrayList<Time> tempAnnotations = TimeUtils.annotateTime(sentence, temporalPipeline, "XXXX-XX-XX");
                    String objDate = "";
                    for (Time tm: tempAnnotations) {
                        for (IndexedWord word: tm.getTemporalWords().getAllWords()) {
                            for (TokenLinked objWord: triple.getObject()) {
                                if (word.index() == objWord.getIndex()) {
                                    objDate = tm.getValue();
                                    break;
                                }
                            }
                        }
                    }
                    if (objDate != null) {
                        String objDateFinal = objDate.replaceAll("X", "#");
                        if (objDateFinal.length() == 4) {
                            objDateFinal += "-##-##";
                        }
                        if (objDateFinal.length() == 7) {
                            objDateFinal += "-##";
                            System.out.print("");
                        }
                        if (objDateFinal.length() != 10) {
                            System.out.print("");
                        }
                        for (Tuple3<String, String,String> fact: yagoFacts) {
                            String factDate = fact._2().substring(1, 11);
                            if (objDateFinal.length() < 4) continue;

                            if (objDateFinal.substring(0,4).equals(factDate.substring(0,4))) {
                                oieDateYearHitCount++;
                                if (objDateFinal.substring(0, 4).equals("XXXX")) {
                                    oieDateYearXXHitCount++;
                                }
                            } else {
                                System.out.print("");
                            }

                            if (objDateFinal.length() < 7) continue;
                            if (objDateFinal.substring(0, 7).equals(factDate.substring(0, 7))) {
                                oieDateMonthHitCount++;
                                if (objDateFinal.substring(4, 7).equals("-XX")) {
                                    oieDateMonthXXHitCount++;
                                }
                            }

                            if (objDateFinal.length() < 10) continue;
                            if (objDate.equals(factDate)) {
                                oieDateDayHitCount++;
                            }

                            System.out.print("");
                        }
                    }
                    if (yagoFacts.size() > 1) {
                        System.out.print("");
                    }
                }
            }
        }

        logger.info("Total number of yagoDateFacts: " + yagoDateFactCount);
        logger.info("Total number of OIE date facts: " + oieDateFactCount);
        logger.info("Total number of OIE date fact hits with YAGO: " + oieDateFactHits);
        logger.info("Year Hits: " + oieDateYearHitCount + "\tHits XXXX: " + oieDateYearXXHitCount);
        logger.info("Month hits: " + oieDateMonthHitCount + "\tHits XX: " + oieDateMonthXXHitCount);
        logger.info("Day hits: " + oieDateDayHitCount);
    }
}
