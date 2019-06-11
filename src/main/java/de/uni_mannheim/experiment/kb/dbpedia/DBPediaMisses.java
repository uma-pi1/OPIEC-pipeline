package de.uni_mannheim.experiment.kb.dbpedia;

import avroschema.linked.TripleLinked;
import avroschema.util.TripleLinkedUtils;

import de.uni_mannheim.querykb.KBHitsCounting;
import de.uni_mannheim.querykb.KBQueryUtils;
import de.uni_mannheim.querykb.query.offline.RelsBetweenWikiPages;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;

/**
 * @author Kiril Gashteovski
 * @author Sven Hertling
 */

public class DBPediaMisses {
    private static final String WL_DIR = "data/OPIEC-Linked";
    private static final String KG_BASE_DIR = "/KBs/";
    private static final String WRITE_FILE_NO_ONTO = "data/OPIEC-Linked-misses/OPIEC-Linked-triples-no-onto-hits.txt";
    private static final String WRITE_FILE_NO_NOTHING = "data/OPIEC-Linked-misses/OPIEC-Linked-triples-no-hits.txt";

    public static void main(String args[]) throws IOException {
        RelsBetweenWikiPages dbpediaQuery = RelsBetweenWikiPages.createDbpediaFromFiles(Integer.MAX_VALUE,
                KG_BASE_DIR + "mappingbased_objects_en.ttl.bz2",
                KG_BASE_DIR + "infobox_properties_mapped_en.ttl.bz2",
                KG_BASE_DIR + "infobox_properties_en.ttl.bz2");

        File folder = new File(WL_DIR);
        File[] listOfFiles = folder.listFiles();

        FileWriter file = new FileWriter(WRITE_FILE_NO_ONTO);
        BufferedWriter bwNoOnto = new BufferedWriter(file);
        FileWriter fileEmpty = new FileWriter(WRITE_FILE_NO_NOTHING);
        BufferedWriter bwEmpty = new BufferedWriter(fileEmpty);

        // Counters
        int fileCounter = 0;

        // Reusable variables
        TripleLinked triple;
        String subjectLink;
        String objectLink;
        Set<String> properties;

        int tripleCount = 0;

        for (int i = 0; i < listOfFiles.length; i++) {
            // Open avro file with triples
            fileCounter++;
            System.out.println("Process File: " + listOfFiles[i].toString() + " File no: " + fileCounter);
            DatumReader<TripleLinked> userDatumReader = new SpecificDatumReader<>(TripleLinked.class);
            DataFileReader<TripleLinked> dataFileReader = new DataFileReader<>(listOfFiles[i], userDatumReader);

            while (dataFileReader.hasNext()) {
                triple = dataFileReader.next();
                tripleCount++;

                subjectLink = KBHitsCounting.getPageTitle(triple.getSubject());
                objectLink = KBHitsCounting.getPageTitle(triple.getObject());

                properties = dbpediaQuery.getConnectingPropertiesGivenPageTitle(subjectLink, objectLink);

                if (properties.isEmpty()) {
                    bwEmpty.write(TripleLinkedUtils.tripleLinkedToString(triple) + "\n");
                }

                if (!KBQueryUtils.containsOntoRels(properties)) {
                    bwNoOnto.write(TripleLinkedUtils.tripleLinkedToString(triple) + "\n");
                }
            }
        }

        bwNoOnto.close();
        bwEmpty.close();
        System.out.println("Number of triples processed: " + tripleCount);
    }
}
