package de.uni_mannheim.kb.dbpedia;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

/**
 * @author Sven Hertling
 * @author Kiril Gashteovski
 */

public class DBpediaEntities {
    private HashSet<String> entities;

    public DBpediaEntities(boolean keepOriginal, String ... dbpediaFactsPaths) throws IOException {
        this.entities = new HashSet<>();
        for (String dbpediaFactsPath: dbpediaFactsPaths) {
            System.out.println("\tProcessing " + dbpediaFactsPath);
            File file = new File(dbpediaFactsPath);
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            int counter = 0;
            while ((line = br.readLine()) != null) {
                counter++;
                if (counter % 1000000 == 0) {
                    System.out.println("\tTriple no: " + counter);
                }

                String[] lineSplit = line.split(" ");
                if (lineSplit.length < 4) {
                    System.out.println("\tSkipped line: " + line);
                    continue;
                }

                if (keepOriginal) {
                    this.entities.add(lineSplit[0]);
                    this.entities.add(lineSplit[2]);
                } else {
                    this.entities.add(getFormattedLink(lineSplit[0]));
                    this.entities.add(getFormattedLink(lineSplit[2]));
                }

                System.out.print("");
            }
        }
    }

    /**
     * Takes a DBpedia entity link (e.g. http://dbpedia.org/page/Jacques_Chirac) and transforms it
     * into lowercased spaced string (e.g. "jacques chirac").
     * @param originalLink DBpedia entity page
     * @return lowercased spaced string
     */
    public static String getFormattedLink(String originalLink) {
        StringBuilder sb = new StringBuilder();
        String [] linkSplit = originalLink.split("dbpedia.org/resource/");
        if (linkSplit.length == 1) {
            System.out.print("");
            return "NO_LINK_FOUND";
        }
        String link = linkSplit[1].substring(0, linkSplit[1].length()-1);
        for (String part: link.split("_")) {
            sb.append(part.toLowerCase());
            sb.append(" ");
        }
        return sb.toString().trim();
    }

    public HashSet<String> getEntities() {
        return this.entities;
    }
}
