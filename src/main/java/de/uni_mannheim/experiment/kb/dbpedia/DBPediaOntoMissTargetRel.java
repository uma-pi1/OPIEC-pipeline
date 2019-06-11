package de.uni_mannheim.experiment.kb.dbpedia;

import avroschema.linked.TripleLinked;
import avroschema.util.TripleLinkedUtils;
import de.uni_mannheim.querydbpedia.KBHitsCounting;
import de.uni_mannheim.querydbpedia.query.offline.RelsBetweenWikiPages;
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

public class DBPediaOntoMissTargetRel {
    private static final String WL_DIR = "data/OPIEC-Linked";
    private static final String KG_BASE_DIR = "/KBs/";
    private static final String WRITE_FILE_DIR = "data/";

    public static void main(String args[]) throws IOException {
        RelsBetweenWikiPages dbpediaQuery = RelsBetweenWikiPages.createDbpediaFromFiles(Integer.MAX_VALUE,
                KG_BASE_DIR + "mappingbased_objects_en.ttl.bz2",
                KG_BASE_DIR + "infobox_properties_mapped_en.ttl.bz2",
                KG_BASE_DIR + "infobox_properties_en.ttl.bz2");

        String targetRel = "be member of";

        // Open reading files
        int fileCounter = 0;
        File folder = new File(WL_DIR);
        File[] listOfFiles = folder.listFiles();

        // Open writing files for every mode
        File writeFileSafe = new File(WRITE_FILE_DIR + "/" + targetRel + ".txt");
        BufferedWriter bw = new BufferedWriter(new FileWriter(writeFileSafe.getAbsoluteFile()));

        String subjectLink, objectLink;
        Set<String> properties, propertiesRev;
        for (int i = 0; i < listOfFiles.length; i++) {
            // Open avro file with triples
            fileCounter++;
            System.out.println("Process File: " + listOfFiles[i].toString() + " File no: " + fileCounter);
            DatumReader<TripleLinked> userDatumReader = new SpecificDatumReader<>(TripleLinked.class);
            DataFileReader<TripleLinked> dataFileReader = new DataFileReader<>(listOfFiles[i], userDatumReader);
            TripleLinked triple;

            while (dataFileReader.hasNext()) {
                triple = dataFileReader.next();
                String relString = TripleLinkedUtils.relToLemmatizedString(triple).toLowerCase();
                if (relString.equals(targetRel)) {
                    subjectLink = KBHitsCounting.getPageTitle(triple.getSubject());
                    objectLink = KBHitsCounting.getPageTitle(triple.getObject());

                    properties = dbpediaQuery.getConnectingPropertiesGivenPageTitle(subjectLink, objectLink);
                    propertiesRev = dbpediaQuery.getConnectingPropertiesGivenPageTitle(objectLink, subjectLink);

                    boolean dbpediaHits = (containsOntoRel(properties)) || (containsOntoRel(propertiesRev));

                    if (!dbpediaHits) {
                        bw.write(TripleLinkedUtils.tripleLinkedToString(triple) + "\n");
                    }
                }
            }
        }
        bw.close();
    }

    private static boolean containsOntoRel(Set<String> rels) {
        for (String r: rels) {
            if (r.contains("/ontology/")) {
                return true;
            }
        }
        return false;
    }
}
