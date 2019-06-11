package de.uni_mannheim.experiment.kb.dbpedia;

import avroschema.linked.TripleLinked;

import de.uni_mannheim.querykb.KBHitsCounting;
import de.uni_mannheim.querykb.query.offline.RelsBetweenWikiPages;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;

/**
 * @author Kiril Gashteovski
 * @author Sven Hertling
 */

public class DBPediaHits {
    private static final String WL_DIR = "data/OPIEC-Linked";
    private static final String KG_BASE_DIR = "/KBs/";

    public static void main(String args[]) throws IOException {
        long start = System.currentTimeMillis();
        RelsBetweenWikiPages dbpediaQuery = RelsBetweenWikiPages.createDbpediaFromFiles(Integer.MAX_VALUE,
                KG_BASE_DIR + "mappingbased_objects_en.ttl.bz2",
                KG_BASE_DIR + "infobox_properties_mapped_en.ttl.bz2",
                KG_BASE_DIR + "infobox_properties_en.ttl.bz2");

        File folder = new File(WL_DIR);
        File[] listOfFiles = folder.listFiles();
        KBHitsCounting kbHits = new KBHitsCounting(dbpediaQuery);

        // Counters
        int fileCounter = 0;

        for (int i = 0; i < listOfFiles.length; i++) {
            // Open avro file with triples
            fileCounter++;
            System.out.println("Process File: " + listOfFiles[i].toString() + " File no: " + fileCounter);
            DatumReader<TripleLinked> userDatumReader = new SpecificDatumReader<>(TripleLinked.class);
            DataFileReader<TripleLinked> dataFileReader = new DataFileReader<>(listOfFiles[i], userDatumReader);
            TripleLinked triple;

            while (dataFileReader.hasNext()) {
                triple = dataFileReader.next();
                kbHits.updateCounters(triple);
            }
        }

        kbHits.printCounters();

        long end = System.currentTimeMillis();
        System.out.println("\n\nExecution time: " + (end - start) / 1000. + "s");
    }
}
