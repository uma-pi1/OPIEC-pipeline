package de.uni_mannheim.experiment.kb.yago.spotl;

import avroschema.linked.TokenLinked;
import avroschema.linked.TripleLinked;
import avroschema.util.TokenLinkedUtils;
import avroschema.util.TripleLinkedUtils;
import avroschema.util.Utils;
import de.uni_mannheim.constant.NE_TYPE;
import de.uni_mannheim.utils.RedirectLinksMap;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;

import java.io.*;
import java.util.HashMap;
import java.util.List;

/**
 * @author Kiril Gashteovski
 * @author Sven Hertling
 */

public class CheckOIEToSPOTLMetaFacts {
    private static final Logger logger = LoggerFactory.getLogger(CheckOIEToSPOTLMetaFacts.class);
    private static final String OPIEC_LINKED_DIR = "data/OPIEC-Linked";
    private static final String SPOTL_FACTS_FILE = "experiments/SPOTL/factsInMetaFacts.txt";
    private static final String TEMP_OIE_FACTS_METATIME = "experiments/SPOTL/oieTemporalMetaDataFacts.txt";
    private static final String TEMP_OIE_FACTS_MODIFIERS = "experiments/SPOTL/oieTemporalModFacts.txt";
    private static final String TEMP_OIE_FACTS_REFERENCES = "experiments/SPOTL/oieTemporalRefFacts.txt";

    public static void main(String args[]) throws IOException {
        // SPOTL facts -- key: "subj_link TAB obj_link", value: (rel, tempPred, time)
        HashMap<String, Tuple3<String, String, String>> spotlFacts = new HashMap<>();

        // Writing files
        File tempMetaDataFile = new File(TEMP_OIE_FACTS_METATIME);
        File tempModFile = new File(TEMP_OIE_FACTS_MODIFIERS);
        File tempRefFile = new File(TEMP_OIE_FACTS_REFERENCES);
        BufferedWriter bwTempMetaData = new BufferedWriter(new FileWriter(tempMetaDataFile.getAbsoluteFile()));
        BufferedWriter bwTempMod = new BufferedWriter(new FileWriter(tempModFile.getAbsoluteFile()));
        BufferedWriter bwTempRef = new BufferedWriter(new FileWriter(tempRefFile.getAbsoluteFile()));

        // Read the SPOTL facts
        BufferedReader brSpotl = new BufferedReader(new FileReader(SPOTL_FACTS_FILE));
        String line = brSpotl.readLine();
        String [] lineSplit;
        String subj, rel, obj, tempPred, time;
        logger.info("Loading SPOTL facts ... ");
        do {
            lineSplit = line.split("\t");
            subj = lineSplit[1];
            rel = lineSplit[2];
            obj = lineSplit[3];
            tempPred = lineSplit[6];
            time = lineSplit[7];
            spotlFacts.put(subj + "\t" + obj, new Tuple3<>(rel, tempPred, time));
            line = brSpotl.readLine();
        } while (line != null);
        logger.info("SPOTL facts loaded!");

        // OIE linked triples (count hits)
        File folder = new File(OPIEC_LINKED_DIR);
        File[] listOfFiles = folder.listFiles();
        String keyCheck, subjLink, objLink, openRel;
        logger.info("Counting OIE triples hits with YAGO meta-data ... ");
        int tripleHits = 0;
        int tripleHitsTemporal = 0;
        int tripleHitsSpatial = 0;
        int tripleMiss = 0;
        int tripleMissesTemporal = 0;
        int tripleMissesSpatial = 0;
        for (int i = 0; i < listOfFiles.length; i++) {
            // Open avro file with triples
            System.out.println("Process File: " + listOfFiles[i].toString() + " File no: " + i);
            DatumReader<TripleLinked> userDatumReader = new SpecificDatumReader<>(TripleLinked.class);
            DataFileReader<TripleLinked> dataFileReader = new DataFileReader<>(listOfFiles[i], userDatumReader);
            TripleLinked triple;

            while (dataFileReader.hasNext()) {
                triple = dataFileReader.next();
                subjLink = getPageTitle(triple.getSubject());
                objLink = getPageTitle(triple.getObject());

                // Define the canonical links
                String subjCanonicalLink;
                String objCanonicalLink;
                HashMap<String, String> canonicalLinksUnderscored = RedirectLinksMap.getLinksUnderscored(triple.getCanonicalLinks());
                if (canonicalLinksUnderscored.containsKey(subjLink)) {
                    subjCanonicalLink = canonicalLinksUnderscored.get(subjLink);
                } else {
                    subjCanonicalLink = subjLink;
                }
                if (canonicalLinksUnderscored.containsKey(objLink)) {
                    objCanonicalLink = canonicalLinksUnderscored.get(objLink);
                } else {
                    objCanonicalLink = objLink;
                }

                keyCheck = subjCanonicalLink + "\t" + objCanonicalLink;

                if (Utils.hasTemporalAnnotation(triple)) {
                    System.out.print("");
                }

                if (spotlFacts.containsKey(keyCheck)) {
                    tripleHits++;
                    if (Utils.hasTemporalAnnotation(triple)) {
                        tripleHitsTemporal++;
                    }
                    if (Utils.hasSpatialAnnotation(triple)) {
                        tripleHitsSpatial++;
                    }
                    Tuple3<String, String, String> value = spotlFacts.get(keyCheck);
                    System.out.print("");
                } else {
                    tripleMiss++;
                    if (Utils.hasTemporalAnnotation(triple)) {
                        // Write temporal metadata triples
                        if (triple.getTimeLinked() != null) {
                            bwTempMetaData.write(TripleLinkedUtils.tripleLinkedToString(triple) + "\n");
                        }

                        // Write temporal modifiers triples
                        boolean hasSubjTime = !triple.getSubjTimeLinked().getTimeLinked().getAllWords().isEmpty();
                        boolean hasObjTime = !triple.getObjTimeLinked().getTimeLinked().getAllWords().isEmpty();
                        if (hasSubjTime || hasObjTime) {
                            bwTempMod.write(TripleLinkedUtils.tripleLinkedToString(triple) + "\n");
                        }

                        // Write temporal reference triples
                        boolean hasTempRef = false;
                        for (TokenLinked t: triple.getSubject()) {
                            if (TokenLinkedUtils.tokenIsTemporal(t)) {
                                hasTempRef = true;
                            }
                        }
                        for (TokenLinked t: triple.getRelation()) {
                            if (TokenLinkedUtils.tokenIsTemporal(t)) {
                                hasTempRef = true;
                            }
                        }
                        for (TokenLinked t: triple.getObject()) {
                            if (TokenLinkedUtils.tokenIsTemporal(t)) {
                                hasTempRef = true;
                            }
                        }
                        if (hasTempRef) {
                            bwTempRef.write(TripleLinkedUtils.tripleLinkedToString(triple) + "\n");
                        }

                        tripleMissesTemporal++;
                    }

                    if (Utils.hasSpatialAnnotation(triple)) {
                        tripleMissesSpatial++;
                    }
                }
            }
        }

        logger.info("OIE triples loaded!");
        logger.info("# of OIE triples: " + (tripleHits + tripleMiss));
        logger.info("# of OIE triple hits with YAGO meta-facts: " + tripleHits);
        logger.info("# of OIE triple hits with YAGO meta-facts (temporal): " + tripleHitsTemporal);
        logger.info("# of OIE triple hits with YAGO meta-facts (spatial): " + tripleHitsSpatial);
        logger.info("# of OIE triple misses with YAGO meta-facts: " + tripleMiss);
        logger.info("# of OIE triple misses with YAGO meta-facts (temporal): " + tripleMissesTemporal);
        logger.info("# of OIE triple misses with YAGO meta-facts (spatial): " + tripleMissesSpatial);

        bwTempMetaData.close();
        bwTempMod.close();
        bwTempRef.close();
    }

    private static String getPageTitle(List<TokenLinked> tokenLinkedList){
        if (tokenLinkedList.get(0).getNer().equals(NE_TYPE.QUANTITY)) {
            if (tokenLinkedList.size() == 1) {
                return makeUppercasedPageTitle(tokenLinkedList.get(0).getWLink().getWikiLink());
            } else {
                return makeUppercasedPageTitle(tokenLinkedList.get(1).getWLink().getWikiLink());
            }
        } else {
            return makeUppercasedPageTitle(tokenLinkedList.get(0).getWLink().getWikiLink());
        }
    }

    public static String makeUppercasedPageTitle(String input) {
        if (input.equals("")) {
            return "";
        }

        String uppercased = input.substring(0, 1).toUpperCase() + input.substring(1);
        String[] mwe = uppercased.split(" ");
        if (mwe.length > 1) {
            uppercased = String.join("_", mwe);
        }
        int lastAnchor = uppercased.lastIndexOf("#");
        if (lastAnchor != -1) {
            uppercased = uppercased.substring(0, lastAnchor);
        }

        return uppercased;
    }
}
