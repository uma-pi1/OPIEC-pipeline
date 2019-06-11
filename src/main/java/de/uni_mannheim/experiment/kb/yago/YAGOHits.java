package de.uni_mannheim.experiment.kb.yago;

import avroschema.linked.TripleLinked;
import de.uni_mannheim.querydbpedia.KBHitsCounting;
import de.uni_mannheim.querydbpedia.query.offline.RelsBetweenWikiPages;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * @author Kiril Gashteovski
 * @author Sven Hertling
 */

public class YAGOHits {
    private static final String WL_DIR = "experiments/OPIEC-Linked";
    private static final String KG_BASE_DIR = "/KBs/";

    public static void main(String args[]) throws IOException {
        System.out.println("LOADING ... ");
        RelsBetweenWikiPages yagoQuery = RelsBetweenWikiPages.createYagoFromFiles(Integer.MAX_VALUE, KG_BASE_DIR + "yagoFacts.tsv");
        System.out.println("RUNNING ... ");

        File folder = new File(WL_DIR);
        File[] listOfFiles = folder.listFiles();
        KBHitsCounting kbHits = new KBHitsCounting(yagoQuery);

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

                String subjLink = KBHitsCounting.getPageTitle(triple.getSubject());
                String objLink = KBHitsCounting.getPageTitle(triple.getObject());
                Set<String> properties = yagoQuery.getConnectingPropertiesGivenPageTitle(subjLink, objLink);

                if (properties.size() > 0) {
                    System.out.print("");
                }

                kbHits.updateCounters(triple);
            }
        }

        kbHits.printCounters();
    }
}
