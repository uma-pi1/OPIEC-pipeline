package de.uni_mannheim.experiment.kb.dbpedia;

import avroschema.linked.TokenLinked;
import avroschema.linked.TripleLinked;

import avroschema.util.TokenLinkedUtils;
import de.uni_mannheim.constant.NE_TYPE;
import de.uni_mannheim.querydbpedia.query.offline.RelsBetweenWikiPages;

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
 * NOTE: this script is used to create content for DBpedia relation clusters
 */
public class DBPediaOntoRelClustering {
    private static final String WL_DIR = "data/OPIEC-Linked";
    private static final String KG_BASE_DIR = "/KBs/";
    private static final String ontoPlaceHolder = "/ontology/";
    private static final String writeFileDir = "experiments/rel_clusters/dbpedia";
    private static final HashMap<String, HashMap<String, Integer>> ontoRelCounts = new HashMap<>();

    public static void main(String args[]) throws IOException {
        // Counters
        int tripleCount = 0;
        int fileCounter = 0;
        int triplehits = 0;

        // Load query object for DBPedia
        long start = System.currentTimeMillis();
        RelsBetweenWikiPages dbpediaQuery = queryDBPediaRelsBetweenWikiPages();
        System.out.println("Execution time: " + (System.currentTimeMillis() - start) / 1000. + "s");


        // Open avro files
        Set<String> properties, inversedProperties, dbpediaRels;
        File [] avroFiles = getAllAvroFiles();
        for (int i = 0; i < avroFiles.length; i++) {
            fileCounter++;
            System.out.println("File: " + avroFiles[i].getName() + "; File no: " + fileCounter);

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

                properties = dbpediaQuery.getConnectingPropertiesGivenPageTitle(subjCanonicalLink, objCanonicalLink);
                inversedProperties = dbpediaQuery.getConnectingPropertiesGivenPageTitle(objCanonicalLink, subjCanonicalLink);

                // If there are onto properties in the "straight" and "inverse" hits, write them in the HashMap
                boolean ontohit = false;
                boolean reverseHit = false;
                if (properties.size() > 0 || inversedProperties.size() > 0) {
                    if (containsOntoRel(properties)) {
                        updateOntoRelCounts(properties, triple);
                        ontohit = true;
                    }
                    if (containsOntoRel(inversedProperties)) {
                        updateOntoRelCounts(inversedProperties, triple);
                        reverseHit = true;
                    }
                    if (ontohit || reverseHit) {
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
     * Load DBPedia and get query object for getting relations between WikiPages
     * @return query object RelsBetweenWikiPages
     */
    private static RelsBetweenWikiPages queryDBPediaRelsBetweenWikiPages() {
        RelsBetweenWikiPages dbpediaQuery = RelsBetweenWikiPages.createDbpediaFromFiles(Integer.MAX_VALUE,
                KG_BASE_DIR + "mappingbased_objects_en.ttl.bz2",
                KG_BASE_DIR + "infobox_properties_mapped_en.ttl.bz2",
                KG_BASE_DIR + "infobox_properties_en.ttl.bz2");
        System.err.println("DONE!");

        return dbpediaQuery;
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

    private static String makeUppercasedPageTitle(String input) {
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

    private static boolean containsOntoRel(Set<String> rels) {
        for (String r: rels) {
            if (r.contains(ontoPlaceHolder)) {
                return true;
            }
        }
        return false;
    }

    private static void updateOntoRelCounts(Set<String> properties, TripleLinked triple) {
        if (!containsOntoRel(properties)) {
            return;
        }

        String relLemmatized = TokenLinkedUtils.linkedTokensLemmasToString(triple.getRelation()).toLowerCase();

        for (String prop: properties) {
            if (prop.contains(ontoPlaceHolder)) {
                if (ontoRelCounts.containsKey(prop)) {
                    if (ontoRelCounts.get(prop).containsKey(relLemmatized)) {
                        int updateCount = ontoRelCounts.get(prop).get(relLemmatized) + 1;
                        ontoRelCounts.get(prop).put(relLemmatized, updateCount);
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
    }

    private static void writeRelationClusters() throws IOException {
        for (String key1 : ontoRelCounts.keySet()) {
            // Open a writing file
            String[] relSplit = key1.split("/");
            File writeFileOntoRels = new File(writeFileDir + "/ontology/regular/" + relSplit[relSplit.length - 1] + ".txt");
            FileWriter fwOntoRels = new FileWriter(writeFileOntoRels.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fwOntoRels);

            for (String key2 : ontoRelCounts.get(key1).keySet()) {
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
