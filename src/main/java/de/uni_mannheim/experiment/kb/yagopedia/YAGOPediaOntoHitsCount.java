package de.uni_mannheim.experiment.kb.yagopedia;

import avroschema.linked.TripleLinked;

import de.uni_mannheim.querykb.KBHitsCounting;
import de.uni_mannheim.querykb.query.offline.RelsBetweenWikiPages;

import de.uni_mannheim.utils.RedirectLinksMap;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

/**
 * @author Kiril Gashteovski
 * @author Sven Hertling
 */

public class YAGOPediaOntoHitsCount {
    private static final String KG_BASE_DIR = "/home/gkiril/Documents/workspace/KBs/minie/kg/kb_in_resource_folder/";
    private static final String WL_DIR = "data/OPIEC-Linked-triples";
    private static final String ONTO_PLACE_HOLDER = "/ontology/";
    private static final String DB_HIT_WRITE_DIR = "data/OPIEC-Linked-kb-hits";

    public static void main(String args[]) throws IOException {
        // Load KBs
        int maxTriples = Integer.MAX_VALUE;
        RelsBetweenWikiPages dbpediaQuery = RelsBetweenWikiPages.createDbpediaFromFiles(maxTriples,
                KG_BASE_DIR + "mappingbased_objects_en.ttl.bz2",
                KG_BASE_DIR + "infobox_properties_mapped_en.ttl.bz2", //maybe not?
                KG_BASE_DIR + "infobox_properties_en.ttl.bz2");
        RelsBetweenWikiPages yagoQuery = RelsBetweenWikiPages.createYagoFromFiles(maxTriples, KG_BASE_DIR + "yagoFacts.tsv");

        // Open reading files
        int fileCounter = 0;
        File folder = new File(WL_DIR);
        File [] listOfFiles = folder.listFiles();

        int tripleCount = 0;
        int dbpediaHitCount = 0, yagoHitCount = 0;
        int dbpediaAndYagoHitCount = 0, dbpediaOrYagoHitCount = 0;

        for (int i = 0; i < listOfFiles.length; i++) {
            // Open avro file with triples
            fileCounter++;
            String filename = listOfFiles[i].toString().split("/")[2];
            System.out.println("Process File: " + listOfFiles[i].toString() + " File no: " + fileCounter);
            DatumReader<TripleLinked> userDatumReader = new SpecificDatumReader<>(TripleLinked.class);
            DataFileReader<TripleLinked> dataFileReader = new DataFileReader<>(listOfFiles[i], userDatumReader);
            TripleLinked triple;

            // Open avro for writing
            Schema schema = new Schema.Parser().parse(new File("avroschema/TripleLinked.avsc"));
            DatumWriter<TripleLinked> userDatumWriter = new SpecificDatumWriter<>(TripleLinked.class);
            DataFileWriter<TripleLinked> dataFileWriter = new DataFileWriter<>(userDatumWriter);
            dataFileWriter.create(schema, new File(DB_HIT_WRITE_DIR + "/AlignedTriples-" + filename));

            while (dataFileReader.hasNext()) {
                triple = dataFileReader.next();
                tripleCount++;

                // Get the argument links + kb rels
                String subjectLink = KBHitsCounting.getPageTitle(triple.getSubject());
                String objectLink = KBHitsCounting.getPageTitle(triple.getObject());

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

                Set<String> dbpediaProps = dbpediaQuery.getConnectingPropertiesGivenPageTitle(subjCanonicalLink, objCanonicalLink);
                Set<String> dbpediaPropsRev = dbpediaQuery.getConnectingPropertiesGivenPageTitle(objCanonicalLink, subjCanonicalLink);
                Set<String> yagoProps = yagoQuery.getConnectingPropertiesGivenPageTitle(subjCanonicalLink, objCanonicalLink);
                Set<String> yagoPropsRev = yagoQuery.getConnectingPropertiesGivenPageTitle(objCanonicalLink, subjCanonicalLink);

                // DBPedia OR YAGO hits counts
                boolean dbHit = (relsContainOnto(dbpediaProps) || relsContainOnto(dbpediaPropsRev));
                boolean ygHit = (yagoProps.size() > 0) || (yagoPropsRev.size() > 0);
                boolean dbpediaOrYago = dbHit || ygHit;
                boolean dbpediaAndYago = dbHit && ygHit;

                // Increment counters
                if (dbHit) {
                    dbpediaHitCount++;
                }
                if (ygHit) yagoHitCount++;
                if (dbpediaOrYago) {
                    dbpediaOrYagoHitCount++;
                    dataFileWriter.append(triple);
                }
                if (dbpediaAndYago) dbpediaAndYagoHitCount++;
            }
            dataFileWriter.close();
        }

        System.out.println("\n\nTriples count: " + tripleCount);
        System.out.println("DBPedia hits count: " + dbpediaHitCount + "; " + String.format("%.3f", (double) dbpediaHitCount / (double) tripleCount));
        System.out.println("YAGO hits count: " + yagoHitCount + "; " + String.format("%.3f", (double) yagoHitCount / (double) tripleCount));
        System.out.println("YAGO or DBpedia hits count: " + dbpediaOrYagoHitCount + "; " + String.format("%.3f",(double) dbpediaOrYagoHitCount / (double) tripleCount));
        System.out.println("YAGO and DBpedia hits count: " + dbpediaAndYagoHitCount + "; " + String.format("%.3f", (double) dbpediaAndYagoHitCount / (double) tripleCount));
        System.out.println("\nDONE!!!");
    }

    private static boolean relsContainOnto(Set<String> rels) {
        for (String rel: rels) {
            if (rel.contains(ONTO_PLACE_HOLDER)) {
                return true;
            }
        }
        return false;
    }
}
