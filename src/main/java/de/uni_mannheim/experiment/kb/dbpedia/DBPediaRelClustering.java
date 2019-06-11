package de.uni_mannheim.experiment.kb.dbpedia;

import avroschema.linked.TokenLinked;
import avroschema.linked.TripleLinked;
import avroschema.util.TokenLinkedUtils;

import de.uni_mannheim.constant.NE_TYPE;
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

public class DBPediaRelClustering {
    private static final String WL_DIR = "data/OPIEC-Linked";
    private static final String KG_BASE_DIR = "/KBs/";
    private static final String ontoPlaceHolder = "/ontology/";
    private static final String propPlaceHolder = "/property/";
    private static final String writeFileDir = "experiments/rel_clusters/dbpedia/";
    private static final HashMap<String, HashMap<String, Integer>> ontoRelCounts = new HashMap<>();
    private static final HashMap<String, HashMap<String, Integer>> propRelCounts = new HashMap<>();

    public static void main(String args[]) throws IOException {
        File folder = new File(WL_DIR);
        File[] listOfFiles = folder.listFiles();

        // Counters
        int reflexiveTriple = 0;
        int tripleCount = 0;
        int fileCounter = 0;
        int triplehits = 0;
        int multiplehits = 0;
        int ontohits = 0;
        int prophits = 0;
        int erroneousLink = 0;

        long start = System.currentTimeMillis();
        RelsBetweenWikiPages dbpediaQuery = RelsBetweenWikiPages.createDbpediaFromFiles(Integer.MAX_VALUE,
                KG_BASE_DIR + "mappingbased_objects_en.ttl.bz2",
                KG_BASE_DIR + "infobox_properties_mapped_en.ttl.bz2",
                KG_BASE_DIR + "infobox_properties_en.ttl.bz2");
        long end = System.currentTimeMillis();
        System.err.println("DONE!");
        System.out.println("Execution time: " + (end - start) / 1000. + "s");

        Set<String> properties;
        Set<String> dbpediaRels = new HashSet<>();
        for (int i = 0; i < listOfFiles.length; i++) {
            // Open avro file with triples
            fileCounter++;
            //System.out.println("Process File: " + listOfFiles[i].toString() + " File no: " + fileCounter);
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
                properties = dbpediaQuery.getConnectingPropertiesGivenPageTitle(subjectLink, objectLink);

                if (properties.size() > 0) {
                    triplehits++;
                    if (properties.size() > 1) {
                        multiplehits++;
                    }
                    boolean hasOntoRel = false;
                    boolean hasPropRel = false;
                    for (String prop: properties) {
                        if (prop.contains(ontoPlaceHolder)) {
                            hasOntoRel = true;
                        } else if (prop.contains(propPlaceHolder)) {
                            hasPropRel = true;
                        }
                        if (hasOntoRel) {
                            ontohits++;
                        }
                        if (hasPropRel) {
                            prophits++;
                        }
                        dbpediaRels.add(prop);
                    }
                }

                updateOntoRelCounts(properties, triple);
            }

            /*if (fileCounter == 1000) {
                break;
            }*/
        }

        System.out.println("\n\n---- Total number of triples processed: " + Integer.toString(tripleCount));
        System.out.println("---- Number of reflexive triples: " + Integer.toString(reflexiveTriple));
        System.out.println("---- Number of hits: " + Integer.toString(triplehits));
        System.out.println("---- Number of triples with multiple hits: "  + Integer.toString(multiplehits));
        System.out.println("---- Number of triples with onto properties: " + Integer.toString(ontohits));
        System.out.println("---- Number of triples with prop properties: " + Integer.toString(prophits));
        System.out.println("---- Number of erroneous links: " + Integer.toString(erroneousLink));
        System.out.println("---- Number of DBPedia relations: " + Integer.toString(dbpediaRels.size()));
        System.out.println("---- All DBPedia rels: ");
        for (String rel: dbpediaRels) {
            System.out.print(rel + ", ");
        }

        writeRelationClusters();
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
        if(lastAnchor != -1){
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
            if (prop.contains(ontoPlaceHolder)) {
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

            // For "/property/"
            if (prop.contains(propPlaceHolder)) {
                if (propRelCounts.containsKey(prop)) {
                    if (propRelCounts.get(prop).containsKey(relLemmatized)) {
                        propRelCounts.get(prop).put(relLemmatized, propRelCounts.get(prop).get(relLemmatized) + 1);
                    } else {
                        propRelCounts.get(prop).put(relLemmatized, 1);
                    }
                } else {
                    HashMap<String, Integer> map = new HashMap<>();
                    map.put(relLemmatized, 1);
                    propRelCounts.put(prop, map);
                }
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

        for (String key1: propRelCounts.keySet()) {
            // Open a writing file
            String [] relSplit = key1.split("/");
            File writeFileOntoRels = new File(writeFileDir + "property/regular/" + relSplit[relSplit.length -1] + ".txt");
            FileWriter fwOntoRels = new FileWriter(writeFileOntoRels.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fwOntoRels);

            for (String key2: propRelCounts.get(key1).keySet()) {
                bw.write("(");
                bw.write(key2);
                bw.write(",");
                bw.write(Integer.toString(propRelCounts.get(key1).get(key2)));
                bw.write(")\n");
            }

            bw.close();
        }
    }
}
