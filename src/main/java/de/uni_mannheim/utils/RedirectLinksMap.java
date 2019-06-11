package de.uni_mannheim.utils;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class RedirectLinksMap {
    // Each line is stored in the format "link_1 TAB link_2"
    private ObjectOpenHashSet<String> linkLines;
    private HashMap<String, String> redirectMap;

    /** Opens an empty set of strings (the dictionary) and then loads the dictionary from the resource path **/
    public RedirectLinksMap(String resourcePath) throws IOException {
        this.linkLines = new ObjectOpenHashSet<>();
        this.load(resourcePath);
        this.redirectMap = new HashMap<>();
        this.generateRedirectMap();
    }

    private void generateRedirectMap() {
        String [] lineSplit;
        for (String linkLine: this.linkLines) {
            lineSplit = linkLine.split("\t");
            String link1 = String.join(" ", lineSplit[0].split("_"));
            String link2 = String.join(" ", lineSplit[1].split("_"));
            this.redirectMap.put(link1, link2);
        }
    }

    // Loading the dictionary
    private void load(String resourcePath) throws IOException {
        this.load(this.getInputStreamFromResource(resourcePath));
    }
    private void load(InputStream in) throws IOException {
        DataInput data = new DataInputStream(in);
        String line = data.readLine();
        while (line != null) {
            line = line.trim();
            if (line.length() > 0) {
                this.linkLines.add(line);
            }
            line = data.readLine();
        }
    }
    private InputStream getInputStreamFromResource(String resourceName) throws IOException {
        return this.getClass().getResource(resourceName).openStream();
    }

    public HashMap<String, String> getRedirectMap() {
        return this.redirectMap;
    }

    /**
     * @param map HashMap where key: link, value: canonical link. The links are separated by space
     * @return HashMap where key: link, value: canonical link. The links are separated by underscore
     */
    public static HashMap<String, String> getLinksUnderscored(Map<String, String> map) {
        HashMap<String, String> canonicalLinksUnderscored = new HashMap<>();

        if (map == null || map.isEmpty()) {
            return canonicalLinksUnderscored;
        }

        for (Map.Entry<String, String> item : map.entrySet()) {
            String key = String.join("_", item.getKey().split(" "));
            String value = String.join("_", item.getValue().split(" "));
            canonicalLinksUnderscored.put(key, value);
        }

        return canonicalLinksUnderscored;
    }
}
