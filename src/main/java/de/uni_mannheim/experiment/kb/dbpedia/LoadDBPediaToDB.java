package de.uni_mannheim.experiment.kb.dbpedia;

import de.uni_mannheim.querykb.KBQueryDB;

import java.io.File;
import java.sql.SQLException;

/**
 * @author Kiril Gashteovski
 * @author Sven Hertling
 */

public class LoadDBPediaToDB {
    private static final String KG_BASE_DIR = "/KBs/";

    public static void main(String args[]) throws SQLException {
        System.out.println("RUNNING ...");

        //TODO: if you want all triples, do not restrict with max_triples number:
        System.out.println("Creating dbpedia.db ... ");
        KBQueryDB dbpediaQuery = new KBQueryDB(new File("./dbpedia.db"));

        System.out.println("Adding ...");
        dbpediaQuery.addDbpediaLabelFile(KG_BASE_DIR + "labels_en.ttl.bz2");
        System.out.println(KG_BASE_DIR + "labels_en.ttl.bz2" + " added ... ");
        dbpediaQuery.addDbpediaDisambiguationFile(KG_BASE_DIR + "disambiguations_en.ttl.bz2");
        System.out.println(KG_BASE_DIR + "disambiguations_en.ttl.bz2" + " added ... ");
        dbpediaQuery.addDbpediaRedirectFile(KG_BASE_DIR + "transitive_redirects_en.ttl.bz2");
        System.out.println(KG_BASE_DIR + "transitive_redirects_en.ttl.bz2" + " added ... ");
        System.out.println("Adding triple files ... ");
        dbpediaQuery.addDbpediaTripleFiles(KG_BASE_DIR + "mappingbased_objects_en.ttl.bz2",
                KG_BASE_DIR + "infobox_properties_mapped_en.ttl.bz2",
                KG_BASE_DIR + "infobox_properties_en.ttl.bz2");
        System.out.println("Triple files added: mappingbased_objects_en.ttl.bz2, infobox_properties_mapped_en.ttl.bz2 " +
                "and infobox_properties_en.ttl.bz2");

        System.out.println("\n\nDONE!!!");
    }
}
