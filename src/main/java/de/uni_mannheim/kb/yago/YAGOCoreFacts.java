package de.uni_mannheim.kb.yago;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple3;
import scala.Tuple4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * @author Sven Hertling
 * @author Kiril Gashteovski
 */
public class YAGOCoreFacts {
    private static final String FACTS_FILE_PATH = "/KBs/yagoFacts.tsv";
    private static final String DATE_FACTS_FILE_PATH = "/KBs/yagoDateFacts.tsv";
    private static final String LABELS_FILE_PATH = "/KBs/yagoLabels.tsv";
    private static final String LITERAL_FACTS_FILE_PATH = "/KBs/yagoLiteralFacts.tsv";

    private static final Logger logger = LoggerFactory.getLogger(YAGOCoreFacts.class);

    private HashMap<String, Tuple3<String, String, String>> yagoFacts;
    private HashSet<String> yagoFactRels;
    private HashMap<String, Tuple4<String, String, String, String>> yagoDateFacts;
    private HashMap<String, Tuple3<String, String, String>> yagoLabel;
    private HashMap<String, Tuple3<String, String, String>> yagoLiteralFacts;
    private HashSet<String> literalRels;
    private HashSet<String> literals;
    private HashSet<String> ids;

    public YAGOCoreFacts() {
        this.yagoFacts = new HashMap<>();
        this.yagoDateFacts = new HashMap<>();
        this.yagoFactRels = new HashSet<>();
        this.yagoLabel = new HashMap<>();
        this.yagoLiteralFacts = new HashMap<>();
        this.literalRels = new HashSet<>();
        this.literals = new HashSet<>();
        this.ids = new HashSet<>();
    }

    public void loadAllFacts() throws IOException {
        this.loadYagoFacts();
        this.loadYagoDateFacts();
        this.loadYagoLiteralFacts();
        //this.loadYagoLabels();
    }

    public void loadYagoFacts() throws IOException {
        logger.info("Loading YAGO facts ... ");
        BufferedReader brFacts = new BufferedReader(new FileReader(FACTS_FILE_PATH));
        int counter = 0;
        String line = brFacts.readLine();
        String [] lineSplit;
        String id, subj, rel, obj;
        do {
            counter++;
            if (counter % 1000000 == 0) {
                logger.info(Integer.toString(counter) + " facts processed ... ");
            }

            lineSplit = line.split("\t");

            if (lineSplit[0].equals("")) {
                logger.info("Skip line ... ");
                line = brFacts.readLine();
                continue;
            }

            id = lineSplit[0].substring(1, lineSplit[0].length() - 1);
            subj = lineSplit[1].substring(1, lineSplit[1].length() - 1);
            rel = lineSplit[2].substring(1, lineSplit[2].length() - 1);
            obj = lineSplit[3].substring(1, lineSplit[3].length() - 1);

            this.yagoFactRels.add(lineSplit[2]);
            this.yagoFacts.put(id, new Tuple3<>(subj, rel, obj));
            this.ids.add(id);

            line = brFacts.readLine();
        } while (line != null);
        logger.info("Loading YAGO facts done!");
        brFacts.close();
    }

    public void loadYagoDateFacts() throws IOException {
        logger.info("Loading YAGO date-facts ... ");
        BufferedReader brDateFacts = new BufferedReader(new FileReader(DATE_FACTS_FILE_PATH));
        int counter = 0;
        String line = brDateFacts.readLine();
        String id, wikiPage, rel, t1, t2;
        String [] lineSplit;
        do {
            counter++;
            if (counter % 1000000 == 0) {
                logger.info(Integer.toString(counter) + " date facts processed ... ");
            }

            lineSplit = line.split("\t");
            if (lineSplit[0].equals("")) {
                logger.info("Skip line ... ");
                line =  brDateFacts.readLine();
                continue;
            }

            lineSplit = line.split("\t");

            id = lineSplit[0].substring(1, lineSplit[0].length() - 1);
            wikiPage = lineSplit[1].substring(1, lineSplit[1].length() - 1);
            rel = lineSplit[2].substring(1, lineSplit[2].length() - 1);
            t1 = lineSplit[3];
            if (lineSplit.length > 4) {
                t2 = lineSplit[4];
            } else {
                t2 = "";
            }

            this.yagoDateFacts.put(id, new Tuple4<>(wikiPage, rel, t1, t2));
            this.ids.add(id);

            line = brDateFacts.readLine();
        } while (line != null);
        logger.info("YAGO date facts loaded!");

        brDateFacts.close();
    }

    public void loadYagoLiteralFacts() throws IOException {
        logger.info("Loading YAGO literal facts ... ");
        BufferedReader brLiterals = new BufferedReader(new FileReader(LITERAL_FACTS_FILE_PATH));
        int counter = 0;
        String line = brLiterals.readLine();
        String id, wikiPage, literalRel, literal;
        String [] lineSplit;
        do {
            counter++;
            if (counter % 1000000 == 0) {
                logger.info(Integer.toString(counter) + " literal facts processed ... ");
            }

            lineSplit = line.split("\t");
            if (lineSplit[0].equals("")) {
                logger.info("Skip line ... ");
                line =  brLiterals.readLine();
                continue;
            }

            id = lineSplit[0].substring(1, lineSplit[0].length() - 1);
            wikiPage = lineSplit[1].substring(1, lineSplit[1].length() - 1);
            literalRel = lineSplit[2];
            literal = lineSplit[3];

            this.yagoLiteralFacts.put(id, new Tuple3<>(wikiPage, literalRel, literal));
            this.literals.add(literal);
            this.literalRels.add(literalRel);
            this.ids.add(id);

            line = brLiterals.readLine();
        } while (line != null);
        logger.info("Loading YAGO literal facts done!");
        brLiterals.close();
    }

    // Getters
    public HashMap<String, Tuple3<String, String, String>> getYagoFacts() {
        return yagoFacts;
    }
    public HashMap<String, Tuple4<String, String, String, String>> getYagoDateFacts() {
        return yagoDateFacts;
    }

    // Setters
    public void setYagoFacts(HashMap<String, Tuple3<String, String, String>> yagoFacts) {
        this.yagoFacts = yagoFacts;
    }
}
