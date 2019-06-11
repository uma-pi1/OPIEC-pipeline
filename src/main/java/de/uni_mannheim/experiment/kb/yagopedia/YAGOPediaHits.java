package de.uni_mannheim.experiment.kb.yagopedia;

import avroschema.linked.TripleLinked;

import de.uni_mannheim.querydbpedia.KBHitsCounting;
import de.uni_mannheim.querydbpedia.query.offline.RelsBetweenWikiPages;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;

/**
 * Count the hits between links in "DBPedia AND YAGO" and "DBPedia OR YAGO"
 *
 * @author Kiril Gashteovski
 */
public class YAGOPediaHits {
    private static final String KG_BASE_DIR = "/KBs/";
    private static final String WL_DIR = "data/OPIEC-Linked";

    public static void main(String args[]) throws IOException {
        long start = System.currentTimeMillis();

        // Load KBs
        int maxTriples = 10000;
        RelsBetweenWikiPages dbpediaQuery = RelsBetweenWikiPages.createDbpediaFromFiles(maxTriples,
                KG_BASE_DIR + "mappingbased_objects_en.ttl.bz2",
                KG_BASE_DIR + "infobox_properties_mapped_en.ttl.bz2",
                KG_BASE_DIR + "infobox_properties_en.ttl.bz2");
        RelsBetweenWikiPages yagoQuery = RelsBetweenWikiPages.createYagoFromFiles(maxTriples, KG_BASE_DIR + "yagoFacts.tsv");

        // Counting for KBs
        KBHitsCounting dbPediaHits = new KBHitsCounting(dbpediaQuery);
        KBHitsCounting tempDbPediaHits = new KBHitsCounting(dbPediaHits);
        KBHitsCounting yagoHits = new KBHitsCounting(yagoQuery);
        KBHitsCounting tempYagoHits = new KBHitsCounting(yagoHits);
        KBHitsCounting dbPediaAndYAGOHits = new KBHitsCounting();
        KBHitsCounting dbPediaOrYAGOHits = new KBHitsCounting();

        // Open reading files
        int fileCounter = 0;
        File folder = new File(WL_DIR);
        File[] listOfFiles = folder.listFiles();

        for (int i = 0; i < listOfFiles.length; i++) {
            // Open avro file with triples
            fileCounter++;
            System.out.println("Process File: " + listOfFiles[i].toString() + " File no: " + fileCounter);
            DatumReader<TripleLinked> userDatumReader = new SpecificDatumReader<>(TripleLinked.class);
            DataFileReader<TripleLinked> dataFileReader = new DataFileReader<>(listOfFiles[i], userDatumReader);
            TripleLinked triple;

            while (dataFileReader.hasNext()) {
                triple = dataFileReader.next();
                dbPediaHits.updateCounters(triple);
                yagoHits.updateCounters(triple);

                // DBPedia OR YAGO hits counts
                boolean dbpediaOrYago = (dbPediaHits.getTriplehits() != tempDbPediaHits.getTriplehits()) ||
                                        (yagoHits.getTriplehits() != tempYagoHits.getTriplehits());
                boolean dbpediaOrYagoReverse = (dbPediaHits.getReverseHits() != tempDbPediaHits.getReverseHits()) ||
                                        (yagoHits.getReverseHits() != tempYagoHits.getReverseHits());
                if (dbpediaOrYago || dbpediaOrYagoReverse) {
                    dbPediaOrYAGOHits.setReverseOrStraightHits(dbPediaOrYAGOHits.getReverseOrStraightHits() + 1);
                }
                if (dbpediaOrYago) {
                    dbPediaOrYAGOHits.setTriplehits(dbPediaOrYAGOHits.getTriplehits() + 1);

                    // Onto hits
                    boolean ontoHits = (dbPediaHits.getOntohits() != tempDbPediaHits.getOntohits()) ||
                                       (yagoHits.getOntohits() != tempYagoHits.getOntohits());
                    if (ontoHits) {
                        dbPediaOrYAGOHits.setOntohits(dbPediaOrYAGOHits.getOntohits() + 1);
                        // reverse AND straight hits
                        if (dbpediaOrYagoReverse) {
                            dbPediaOrYAGOHits.setReverseAndStraightHits(dbPediaOrYAGOHits.getReverseAndStraightHits() + 1);
                        }
                    }

                    // One onto hits
                    boolean oneOntoHit = (dbPediaHits.getOneOntoHit() != tempDbPediaHits.getOneOntoHit()) ||
                                         (yagoHits.getOneOntoHit() != tempYagoHits.getOneOntoHit());
                    if (oneOntoHit) {
                        dbPediaOrYAGOHits.setOneOntoHit(dbPediaOrYAGOHits.getOneOntoHit() + 1);
                    }

                    // Multiple onto hits
                    boolean multipleOntoHits = (dbPediaHits.getMultipleOntoHits() != tempDbPediaHits.getMultipleOntoHits()) ||
                                               (yagoHits.getMultipleOntoHits() != tempYagoHits.getMultipleOntoHits());
                    if (multipleOntoHits) {
                        dbPediaOrYAGOHits.setMultipleOntoHits(dbPediaOrYAGOHits.getMultipleOntoHits() + 1);
                    }

                    // Prop hits
                    if (dbPediaHits.getProphits() != tempDbPediaHits.getProphits()) {
                        dbPediaOrYAGOHits.setProphits(dbPediaOrYAGOHits.getProphits() + 1);
                    }

                    // One prop hit
                    if (dbPediaHits.getOnePropHit() != tempDbPediaHits.getOnePropHit()) {
                        dbPediaOrYAGOHits.setOnePropHit(dbPediaOrYAGOHits.getOnePropHit() + 1);
                    }
                }
                if (!dbpediaOrYago && dbpediaOrYagoReverse) {
                    dbPediaOrYAGOHits.setReverseHitsOnly(dbPediaOrYAGOHits.getReverseHitsOnly() + 1);
                }

                if (dbpediaOrYagoReverse) {
                    dbPediaOrYAGOHits.setReverseHits(dbPediaOrYAGOHits.getReverseHits() + 1);
                }

                // DBPedia AND YAGO hits counts
                boolean dbpediaAndYago = (dbPediaHits.getTriplehits() != tempDbPediaHits.getTriplehits()) &&
                                         (yagoHits.getTriplehits() != tempYagoHits.getTriplehits());
                boolean dbpediaAndYagoReverse = (dbPediaHits.getReverseHits() != tempDbPediaHits.getReverseHits()) &&
                                                (yagoHits.getReverseHits() != tempYagoHits.getReverseHits());
                if (dbpediaAndYago) {
                    dbPediaAndYAGOHits.setTriplehits(dbPediaAndYAGOHits.getTriplehits() + 1);

                    // Onto hits
                    boolean ontoHits = (dbPediaHits.getOntohits() != tempDbPediaHits.getOntohits()) &&
                                       (yagoHits.getOntohits() != tempYagoHits.getOntohits());
                    if (ontoHits) {
                        dbPediaAndYAGOHits.setOntohits(dbPediaAndYAGOHits.getOntohits() + 1);
                        // reverse AND straight hits
                        if (dbpediaAndYagoReverse) {
                            dbPediaAndYAGOHits.setReverseAndStraightHits(dbPediaAndYAGOHits.getReverseAndStraightHits() + 1);
                        }
                    }

                    // One onto hits
                    boolean oneOntoHit = (dbPediaHits.getOneOntoHit() != tempDbPediaHits.getOneOntoHit()) &&
                                         (yagoHits.getOneOntoHit() != tempYagoHits.getOneOntoHit());
                    if (oneOntoHit) {
                        dbPediaAndYAGOHits.setOneOntoHit(dbPediaAndYAGOHits.getOneOntoHit() + 1);
                    }

                    // Multiple onto hits
                    boolean multipleOntoHits = (dbPediaHits.getMultipleOntoHits() != tempDbPediaHits.getMultipleOntoHits()) &&
                                               (yagoHits.getMultipleOntoHits() != tempYagoHits.getMultipleOntoHits());
                    if (multipleOntoHits) {
                        dbPediaAndYAGOHits.setMultipleOntoHits(dbPediaAndYAGOHits.getMultipleOntoHits() + 1);
                    }

                    // Prop hits
                    if (dbPediaHits.getProphits() != tempDbPediaHits.getProphits() &&
                            yagoHits.getProphits() != tempYagoHits.getProphits()) {
                        dbPediaAndYAGOHits.setProphits(dbPediaAndYAGOHits.getProphits() + 1);
                    }

                    // One prop hit
                    if (dbPediaHits.getOnePropHit() != tempDbPediaHits.getOnePropHit() &&
                            yagoHits.getOnePropHit() != tempYagoHits.getOnePropHit()) {
                        dbPediaAndYAGOHits.setOnePropHit(dbPediaAndYAGOHits.getOnePropHit() + 1);
                    }
                }

                tempDbPediaHits = new KBHitsCounting(dbPediaHits);
                tempYagoHits = new KBHitsCounting(yagoHits);
            }
        }

        System.out.println("\n\nDBPedia hits:");
        System.out.println("==========================================");
        dbPediaHits.printCounters();
        System.out.println("\n\nYAGO hits:");
        System.out.println("==========================================");
        yagoHits.printCounters();
        System.out.println("\n\nDBPedia OR YAGO hits: ");
        System.out.println("==========================================");
        dbPediaOrYAGOHits.printCounters();
        System.out.println("\n\nDBPedia AND YAGO hits: ");
        System.out.println("==========================================");
        dbPediaAndYAGOHits.printCounters();

        long end = System.currentTimeMillis();
        System.out.println("\n\nExecution time: " + (end - start) / 1000. + "s");
    }
}
