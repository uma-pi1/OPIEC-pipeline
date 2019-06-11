package de.uni_mannheim.kb.yago;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

/**
 * @author Sven Hertling
 * @author Kiril Gashteovski
 */
public class YAGOEntities {
    private HashSet<String> entities;

    public YAGOEntities(boolean keepOriginal, String yagoFactsPath) throws IOException {
        this.entities = new HashSet<>();
        File file = new File(yagoFactsPath);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String st;
        while ((st = br.readLine()) != null){
            String [] lineSplit = st.split("\t");
            if (lineSplit[0].equals("")) continue;
            if (keepOriginal) {
                this.entities.add(lineSplit[1]);
                this.entities.add(lineSplit[3]);
            } else {
                this.entities.add(getFormattedLink(lineSplit[1]));
                this.entities.add(getFormattedLink(lineSplit[3]));
            }
        }
    }

    public HashSet<String> getEntities() {
        return this.entities;
    }
    public int getEntitiesSize() {
        return this.entities.size();
    }

    public static String getFormattedLink(String originalLink) {
        StringBuilder sb = new StringBuilder();
        String linkSubstring = originalLink.substring(1, originalLink.length()-1);
        for (String sp: linkSubstring.split("_")) {
            sb.append(sp.toLowerCase());
            sb.append(" ");
        }
        return sb.toString().trim();
    }
}
