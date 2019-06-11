package de.uni_mannheim.kb.yago;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * @author Kiril Gashteovski
 */

public class YAGOMetaFacts {
    private static final Logger logger = LoggerFactory.getLogger(YAGOMetaFacts.class);
    private static final String META_FACTS_FILE_PATH = "/KBs/yagoMetaFacts.tsv";

    /** Map, where key: meta-fact ID; value: tuple3 (factID, predicate, date/space) **/
    private HashMap<String, Tuple3<String, String, String>> metaFacts;
    /** Set of IDs of all the meta-facts **/
    private HashSet<String> metaFactIDs;
    /** All the fact IDs which are part of the meta-facts **/
    private HashSet<String> factIDs;
    /** All the meta-properties (relations) **/
    private HashSet<String> metaProperties;

    public YAGOMetaFacts() {
        this.metaFacts = new HashMap<>();
        this.metaFactIDs = new HashSet<>();
        this.factIDs = new HashSet<>();
        this.metaProperties = new HashSet<>();
    }

    public void loadYagoMetaFacts() throws IOException {
        logger.info("Loading YAGO meta facts ... ");
        BufferedReader brMetaFacts = new BufferedReader(new FileReader(META_FACTS_FILE_PATH));
        int counter = 0;
        String line = brMetaFacts.readLine();
        String [] lineSplit;
        String metaFactId, factId, prop, wikiPage;
        do {
            counter++;
            if (counter % 1000000 == 0) {
                logger.info(Integer.toString(counter) + " meta facts processed ... ");
            }

            lineSplit = line.split("\t");
            if (lineSplit[0].equals("")) {
                logger.info("Skip line ... ");
                line = brMetaFacts.readLine();
                continue;
            }

            metaFactId = lineSplit[0].substring(1, lineSplit[0].length() - 1);
            factId = lineSplit[1].substring(1, lineSplit[1].length() - 1);
            prop = lineSplit[2].substring(1, lineSplit[2].length() - 1);
            wikiPage = lineSplit[3].substring(1, lineSplit[3].length() - 1);

            this.metaFacts.put(metaFactId, new Tuple3<>(factId, prop, wikiPage));
            this.metaProperties.add(prop);
            this.metaFactIDs.add(metaFactId);
            this.factIDs.add(factId);

            line = brMetaFacts.readLine();
        } while (line != null);
        logger.info("Meta facts processing done!");
        brMetaFacts.close();
    }

    // Getters
    public HashMap<String, Tuple3<String, String, String>> getMetaFacts() {
        return metaFacts;
    }
    public HashSet<String> getMetaFactIDs() {
        return metaFactIDs;
    }
    public HashSet<String> getFactIDs() {
        return factIDs;
    }
    public HashSet<String> getMetaProperties() {
        return metaProperties;
    }

    // Setters
    public void setMetaFacts(HashMap<String, Tuple3<String, String, String>> metaFacts) {
        this.metaFacts = metaFacts;
    }
    public void setMetaFactIDs(HashSet<String> metaFactIDs) {
        this.metaFactIDs = metaFactIDs;
    }
    public void setFactIDs(HashSet<String> factIDs) {
        this.factIDs = factIDs;
    }
    public void setMetaProperties(HashSet<String> metaProperties) {
        this.metaProperties = metaProperties;
    }
}
