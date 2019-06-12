package de.uni_mannheim.kb.yago;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

/**
 * @author Sven Hertling
 * @author Kiril Gashteovski
 */
public class YAGOCoreFactsIDs {
    private static final Logger logger = LoggerFactory.getLogger(YAGOCoreFacts.class);
    private static final String FACTS_FILE_PATH = "/KBs/yagoFacts.tsv";
    private static final String DATE_FACTS_FILE_PATH = "/KBs/yagoDateFacts.tsv";
    private static final String LABELS_FILE_PATH = "/KBs/yagoLabels.tsv";
    private static final String LITERAL_FACTS_FILE_PATH = "/KBs/yagoLiteralFacts.tsv";

    private HashSet<String> ids;

    public YAGOCoreFactsIDs() {
        this.ids = new HashSet<>();
    }

    public void loadAllCoreFactIDs() throws IOException {
        this.loadYAGOFactIDs();
        this.loadYAGODateFactIDs();
        this.loadYAGOLabelFactIDs();
        this.loadYAGOLiteralFactIDs();
    }

    public void loadYAGOFactIDs() throws IOException {
        loadFactIDs(FACTS_FILE_PATH);
    }

    public void loadYAGODateFactIDs() throws  IOException {
        loadFactIDs(DATE_FACTS_FILE_PATH);
    }

    public void loadYAGOLabelFactIDs() throws IOException {
        loadFactIDs(LABELS_FILE_PATH);
    }

    public void loadYAGOLiteralFactIDs() throws IOException {
        loadFactIDs(LITERAL_FACTS_FILE_PATH);
    }

    public void loadFactIDs(String filePath) throws IOException {
        logger.info("Loading: " + filePath);
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        int counter = 0;
        String line = br.readLine();
        String [] lineSplit;
        do {
            counter++;
            if (counter % 1000000 == 0) {
                logger.info(Integer.toString(counter) + " facts processed ... ");
            }

            lineSplit = line.split("\t");

            if (lineSplit[0].equals("")) {
                logger.info("Skip line ... ");
                line = br.readLine();
                continue;
            }

            this.ids.add(lineSplit[0].substring(1, lineSplit[0].length() - 1));
            line = br.readLine();
        } while (line != null);
        logger.info("Loading YAGO facts done!");
        br.close();
    }

    public HashSet<String> getIds() {
        return this.ids;
    }
}
