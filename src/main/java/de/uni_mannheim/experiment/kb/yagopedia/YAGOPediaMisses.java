package de.uni_mannheim.experiment.kb.yagopedia;

import avroschema.linked.TripleLinked;
import avroschema.util.TripleLinkedUtils;
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

/**
 * @author Kiril Gashteovski
 * @author Sven Hertling
 */

public class YAGOPediaMisses {
    private static final String KG_BASE_DIR = "/KBs/";
    private static final String WL_DIR = "data/OPIEC-Linked";
    private static final String WRITE_FILE_RELS = "data/KB_misses_rel.txt";
    private static final String WRITE_MISSES_AVRO = "data/OPIEC-Linked-KB_misses";

    public static void main(String args[]) throws IOException {
        // Load KBs
        int maxTriples = Integer.MAX_VALUE;
        RelsBetweenWikiPages dbpediaQuery = RelsBetweenWikiPages.createDbpediaFromFiles(maxTriples,
                KG_BASE_DIR + "mappingbased_objects_en.ttl.bz2",
                KG_BASE_DIR + "infobox_properties_mapped_en.ttl.bz2",
                KG_BASE_DIR + "infobox_properties_en.ttl.bz2");
        RelsBetweenWikiPages yagoQuery = RelsBetweenWikiPages.createYagoFromFiles(maxTriples, KG_BASE_DIR + "yagoFacts.tsv");

        // Open reading files
        int fileCounter = 0;
        File folder = new File(WL_DIR);
        File[] listOfFiles = folder.listFiles();

        // Open writing files for every mode
        File writeFileRels = new File(WRITE_FILE_RELS);
        BufferedWriter bwRels = new BufferedWriter(new FileWriter(writeFileRels.getAbsoluteFile()));

        for (int i = 0; i < listOfFiles.length; i++) {
            // Open avro file with triples
            fileCounter++;
            System.out.println("Process File: " + listOfFiles[i].toString() + " File no: " + fileCounter);
            String filename = listOfFiles[i].toString().split("/")[2];

            // Open avro for reading
            DatumReader<TripleLinked> userDatumReader = new SpecificDatumReader<>(TripleLinked.class);
            DataFileReader<TripleLinked> dataFileReader = new DataFileReader<>(listOfFiles[i], userDatumReader);
            TripleLinked triple;

            // Open avro for writing
            Schema schema = new Schema.Parser().parse(new File("avroschema/TripleLinked.avsc"));
            DatumWriter<TripleLinked> userDatumWriter = new SpecificDatumWriter<>(TripleLinked.class);
            DataFileWriter<TripleLinked> dataFileWriter = new DataFileWriter<>(userDatumWriter);
            dataFileWriter.create(schema, new File(WRITE_MISSES_AVRO + "/NonAlignedTriples-" + filename));

            while (dataFileReader.hasNext()) {
                triple = dataFileReader.next();
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

                boolean dbpediaHits = (containsOntoRel(dbpediaProps)) || (containsOntoRel(dbpediaPropsRev));
                boolean yagoHits = (yagoProps.size() > 0) || (yagoPropsRev.size() > 0);

                if (!dbpediaHits && !yagoHits) {
                    bwRels.write(TripleLinkedUtils.relToLemmatizedString(triple).toLowerCase() + "\n");
                    dataFileWriter.append(triple);
                }
            }
            dataFileWriter.close();
        }

        bwRels.close();
        System.out.println("DONE!!!");
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
