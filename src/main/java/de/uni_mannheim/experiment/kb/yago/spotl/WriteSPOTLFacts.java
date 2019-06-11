package de.uni_mannheim.experiment.kb.yago.spotl;

import de.uni_mannheim.kb.yago.YAGOCoreFacts;
import de.uni_mannheim.kb.yago.YAGOCoreFactsIDs;
import de.uni_mannheim.kb.yago.YAGOMetaFacts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author Kiril Gashteovski
 * @author Sven Hertling
 */

public class WriteSPOTLFacts {
    private static final Logger logger = LoggerFactory.getLogger(WriteSPOTLFacts.class);

    public static void main(String args[]) throws IOException {
        YAGOMetaFacts yagoMetaFacts = new YAGOMetaFacts();
        yagoMetaFacts.loadYagoMetaFacts();

        YAGOCoreFactsIDs yagoFactIDs = new YAGOCoreFactsIDs();
        yagoFactIDs.loadAllCoreFactIDs();

        int countMetaFacts = 0;
        int factsInMetaFacts = 0;
        int factsNotInMetaFacts = 0;
        for (String metafactId: yagoMetaFacts.getMetaFacts().keySet()) {
            countMetaFacts++;
            if (yagoFactIDs.getIds().contains(yagoMetaFacts.getMetaFacts().get(metafactId)._1())) {
                factsInMetaFacts++;
            } else {
                factsNotInMetaFacts++;
            }
        }

        logger.info("# of all yago core facts: " + yagoFactIDs.getIds().size());
        logger.info("# of all yago meta facts: " + yagoMetaFacts.getMetaFacts().size());
        logger.info("# Meta facts: " + countMetaFacts);
        logger.info("# facts in meta facts: " + factsInMetaFacts);
        logger.info("# facts not in meta facts: " + factsNotInMetaFacts);

        YAGOCoreFacts yagoCoreFacts = new YAGOCoreFacts();
        yagoCoreFacts.loadAllFacts();

        factsInMetaFacts = 0;
        factsNotInMetaFacts = 0;
        File writeFileFactsInMetaFacts = new File("experiments/SPOTL/factsInMetaFacts.txt");
        File writeFileFactsNotInMetaFacts = new File("experiments/SPOTL/factsNotInMetaFacts.txt");
        BufferedWriter bwFactsInMetaFacts = new BufferedWriter(new FileWriter(writeFileFactsInMetaFacts.getAbsoluteFile()));
        BufferedWriter bwFactsNotInMetaFacts = new BufferedWriter(new FileWriter(writeFileFactsNotInMetaFacts.getAbsoluteFile()));
        logger.info("Writing metafacts ...");

        for (String metafactId: yagoMetaFacts.getMetaFacts().keySet()) {
            String factId = yagoMetaFacts.getMetaFacts().get(metafactId)._1();
            if (yagoCoreFacts.getYagoFacts().get(factId) != null) {
                factsInMetaFacts++;
                // Write the fact
                bwFactsInMetaFacts.write(factId + "\t");
                bwFactsInMetaFacts.write(yagoCoreFacts.getYagoFacts().get(factId)._1() + "\t");
                bwFactsInMetaFacts.write(yagoCoreFacts.getYagoFacts().get(factId)._2() + "\t");
                bwFactsInMetaFacts.write(yagoCoreFacts.getYagoFacts().get(factId)._3() + "\t");

                // Write the metafact
                Tuple3<String, String, String> metafact = yagoMetaFacts.getMetaFacts().get(metafactId);
                bwFactsInMetaFacts.write(metafactId + "\t");
                bwFactsInMetaFacts.write(metafact._1() + "\t" + metafact._2() + "\t" + metafact._3() + "\n");
            } else {
                factsNotInMetaFacts++;

                Tuple3<String, String, String> metafact = yagoMetaFacts.getMetaFacts().get(metafactId);
                bwFactsNotInMetaFacts.write(metafact._1() + "\t" + metafact._2() + "\t" + metafact._3() + "\n");
            }
        }
        logger.info("# facts in meta facts: " + factsInMetaFacts);
        logger.info("# facts not in meta facts: " + factsNotInMetaFacts);

        bwFactsInMetaFacts.close();
        bwFactsNotInMetaFacts.close();

        logger.info("\n\nDONE!!!");
    }
}
