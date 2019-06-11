package de.uni_mannheim.experiment.kb.yago;

import avroschema.linked.TokenLinked;
import avroschema.linked.TripleLinked;
import avroschema.util.TokenLinkedUtils;
import de.uni_mannheim.constant.NE_TYPE;
import de.uni_mannheim.querykb.query.offline.RelsBetweenWikiPages;
import de.uni_mannheim.utils.RedirectLinksMap;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * /**
 * @author Kiril Gashteovski
 * @author Sven Hertling
 *
 * NOTE: this script is used to create content for YAGO relation clusters
 */

public class YAGOOntoRelClustering {
    private static final String WL_DIR = "experiments/OPIEC-Linked";
    private static final String KG_BASE_DIR = "/KBs/";
    private static final String writeFileDir = "experiments/rel_clusters/yago";
    private static final HashMap<String, HashMap<String, Integer>> ontoRelCounts = new HashMap<>();

    public static void main(String args[]) throws IOException {
        // Counters
        int tripleCount = 0;
        int fileCounter = 0;
        int triplehits = 0;

        // Load query object for YAGO
        long start = System.currentTimeMillis();
        RelsBetweenWikiPages yagoQuery = RelsBetweenWikiPages.createYagoFromFiles(Integer.MAX_VALUE,KG_BASE_DIR + "yagoFacts.tsv");
        System.out.println("Execution time: " + (System.currentTimeMillis() - start) / 1000. + "s");

        Set<String> properties, inverseProperties;
        File [] avroFiles = getAllAvroFiles();
        for (int i = 0; i < avroFiles.length; i++) {
            // Open avro file with triples
            fileCounter++;
            System.out.println("Process File: " + avroFiles[i].toString() + " File no: " + fileCounter);

            // Open avro file with triples
            TripleLinked triple;
            DataFileReader<TripleLinked> dataFileReader = getDataFileReader(avroFiles[i]);
            while (dataFileReader.hasNext()) {
                triple = dataFileReader.next();
                tripleCount++;

                String subjectLink = getPageTitle(triple.getSubject());
                String objectLink = getPageTitle(triple.getObject());

                // Define the canonical links
                String subjCanonicalLink;
                String objCanonicalLink;
                HashMap<String, String> canonicalLinksUnderscored = RedirectLinksMap.getLinksUnderscored(triple.getCanonicalLinks());
                if (canonicalLinksUnderscored.containsKey(subjectLink)) {
                    subjCanonicalLink = canonicalLinksUnderscored.get(subjectLink);
                } else {
                    subjCanonicalLink = subjectLink;
                }
                if (canonicalLinksUnderscored.containsKey(objectLink)) {
                    objCanonicalLink = canonicalLinksUnderscored.get(objectLink);
                } else {
                    objCanonicalLink = objectLink;
                }

                properties = yagoQuery.getConnectingPropertiesGivenPageTitle(subjCanonicalLink, objCanonicalLink);
                inverseProperties = yagoQuery.getConnectingPropertiesGivenPageTitle(objCanonicalLink, subjCanonicalLink);

                boolean hit = false, reverseHit = false;
                if (properties.size() > 0 || inverseProperties.size() > 0) {
                    if (properties.size() > 0) {
                        hit = true;
                        updateOntoRelCounts(properties, triple);
                    }
                    if (inverseProperties.size() > 0) {
                        reverseHit = true;
                        updateOntoRelCounts(inverseProperties, triple);
                    }

                    if (hit || reverseHit) {
                        triplehits++;
                    }
                }
            }
        }

        System.out.println("\nWriting rel clusters ... ");
        writeRelationClusters();
        System.out.println("Writing rel clusters done!");

        System.out.println("\n\nNumber of triples: " + tripleCount);
        System.out.println("Number of triple hits: " + triplehits);
    }

    /**
     * @return list of AVRO files, given the WL directory
     */
    private static File [] getAllAvroFiles() {
        File folder = new File(WL_DIR);
        return folder.listFiles();
    }

    /**
     * @param avroFile the input avro file
     * @return data file reader
     * @throws IOException
     */
    private static DataFileReader<TripleLinked> getDataFileReader(File avroFile) throws IOException {
        DatumReader<TripleLinked> userDatumReader = new SpecificDatumReader<>(TripleLinked.class);
        return new DataFileReader<>(avroFile, userDatumReader);
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

    private static void updateOntoRelCounts(Set<String> properties, TripleLinked triple) {
        if (properties.size() == 0) {
            return;
        }

        String relLemmatized = TokenLinkedUtils.linkedTokensLemmasToString(triple.getRelation()).toLowerCase();

        for (String prop: properties) {
            // For ontological properties
            if (ontoRelCounts.containsKey(prop)) {
                if (ontoRelCounts.get(prop).containsKey(relLemmatized)) {
                    ontoRelCounts.get(prop).put(relLemmatized, ontoRelCounts.get(prop).get(relLemmatized) + 1);
                } else {
                    ontoRelCounts.get(prop).put(relLemmatized, 1);
                }
            } else {
                HashMap<String, Integer> map = new HashMap<>();
                map.put(relLemmatized, 1);
                ontoRelCounts.put(prop, map);
            }
        }
    }

    private static void writeRelationClusters() throws IOException {
        for (String key1: ontoRelCounts.keySet()) {
            // Open a writing file
            String [] relSplit = key1.split("/");
            File writeFileOntoRels = new File(writeFileDir + "/ontology/regular/" + relSplit[relSplit.length -1] + ".txt");
            FileWriter fwOntoRels = new FileWriter(writeFileOntoRels.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fwOntoRels);

            for (String key2: ontoRelCounts.get(key1).keySet()) {
                bw.write("(");
                bw.write(key2);
                bw.write(",");
                bw.write(Integer.toString(ontoRelCounts.get(key1).get(key2)));
                bw.write(")\n");
            }

            bw.close();
        }
    }
}
