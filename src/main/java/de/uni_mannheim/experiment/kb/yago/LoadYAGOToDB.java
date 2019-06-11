package de.uni_mannheim.experiment.kb.yago;

import de.uni_mannheim.querykb.KBQueryDB;

import java.io.File;
import java.sql.SQLException;

/**
 * @author Kiril Gashteovski
 * @author Sven Hertling
 */

public class LoadYAGOToDB {
    private static final String KG_BASE_DIR = "/KBs/";

    public static void main(String args[]) throws SQLException {
        System.out.println("Creating yago.db ... ");
        System.out.println("Adding ...");
        KBQueryDB yagoQuery = new KBQueryDB(new File("./yago.db"));
        yagoQuery.addYagoTripleFile(KG_BASE_DIR + "yagoFacts.tsv");

        System.out.println("\n\nDONE!!!");
    }
}
