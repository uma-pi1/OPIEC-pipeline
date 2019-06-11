package de.uni_mannheim.experiment.kb.yago;

import avroschema.linked.TokenLinked;
import avroschema.linked.TripleLinked;
import avroschema.util.TokenLinkedUtils;

import de.uni_mannheim.constant.NE_TYPE;
import de.uni_mannheim.querydbpedia.KBCounting;
import de.uni_mannheim.querydbpedia.query.offline.RelsBetweenWikiPages;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Kiril Gashteovski
 * @author Sven Hertling
 */

public class YAGORelClustering {
    private static final String WL_DIR = "data/OPIEC-Linked";
    private static final String KG_BASE_DIR = "/KBs/";
    private static final String writeFileDir = "experiments/rel_clusters/yago/";
    private static final HashMap<String, HashMap<String, Integer>> ontoRelCounts = new HashMap<>();
    private static final Set<String> yagorels = new HashSet<>();

    public static void main(String args[]) throws IOException {
        File folder = new File(WL_DIR);
        File[] listOfFiles = folder.listFiles();

        // Counters
        int reflexiveTriple = 0;
        int tripleCount = 0;
        int fileCounter = 0;
        int erroneousLink = 0;
        int triplehits = 0;
        int multiplehits = 0;
        Set<String> yagorels = new HashSet<>();

        long start = System.currentTimeMillis();
        RelsBetweenWikiPages yagoQuery = RelsBetweenWikiPages.createYagoFromFiles(Integer.MAX_VALUE,
                KG_BASE_DIR + "yagoFacts.tsv");
        long end = System.currentTimeMillis();
        System.err.println("DONE!");
        System.out.println("Execution time: " + (end - start) / 1000. + "s");

        Set<String> properties;
        for (int i = 0; i < listOfFiles.length; i++) {
            // Open avro file with triples
            fileCounter++;
            System.out.println("Process File: " + listOfFiles[i].toString() + " File no: " + fileCounter);
            DatumReader<TripleLinked> userDatumReader = new SpecificDatumReader<>(TripleLinked.class);
            DataFileReader<TripleLinked> dataFileReader = new DataFileReader<>(listOfFiles[i], userDatumReader);
            TripleLinked triple;

            while (dataFileReader.hasNext()) {
                triple = dataFileReader.next();
                tripleCount++;

                String subjectLink = getPageTitle(triple.getSubject());
                String objectLink = getPageTitle(triple.getObject());

                if (subjectLink.equals("") || objectLink.equals("")) {
                    erroneousLink++;
                    tripleCount--;
                    continue;
                }

                if (subjectLink.equals(objectLink)) {
                    reflexiveTriple++;
                    continue;
                }
                properties = yagoQuery.getConnectingPropertiesGivenPageTitle(subjectLink, objectLink);

                if (properties.size() > 0) {
                    triplehits++;
                    if (properties.size() > 1) {
                        multiplehits++;
                    }
                    for (String prop: properties) {
                        yagorels.add(prop);
                    }
                }

                updateOntoRelCounts(properties, triple);
            }
        }

        System.out.println("\n\n---- Total number of triples processed: " + Integer.toString(tripleCount));
        System.out.println("---- Number of reflexive triples: " + Integer.toString(reflexiveTriple));
        System.out.println("---- Number of hits: " + Integer.toString(triplehits));
        System.out.println("---- Number of triples with multiple hits: "  + Integer.toString(multiplehits));
        System.out.println("---- Number of erroneous links: " + Integer.toString(erroneousLink));
        System.out.println("---- Number of YAGO rels: " + Integer.toString(yagorels.size()));
        System.out.println("---- All YAGO rels:");
        for (String rel: yagorels) {
            System.out.print(rel + ", ");
        }

        writeRelationClusters();
        System.out.println("\n\nAll YAGO rels:");
        for (String rel: yagorels) {
            System.out.println(rel);
        }
        System.out.println("\nWriting ...");
        writeRelationClusters();
        System.out.println("\nDONE!");

        System.out.print("");
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

        String relLemmatized = TokenLinkedUtils.linkedTokensLemmasToString(triple.getRelation());

        for (String prop: properties) {
            yagorels.add(prop);
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
            File writeFileOntoRels = new File(writeFileDir + "ontology/regular/" + relSplit[relSplit.length -1] + ".txt");
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
