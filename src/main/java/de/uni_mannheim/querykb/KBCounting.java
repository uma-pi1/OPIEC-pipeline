/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.querykb;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Sven Hertling
 * @author Kiril Gashteovski
 */
public class KBCounting {
    private static final String newline = System.getProperty("line.separator");

    private int hits;
    private int multipleProperties;
    private Map<String, Set<String>> propToText;

    private StringBuilder logs;

    public KBCounting(){
        this.hits = 0;
        this.multipleProperties = 0;
        this.logs = new StringBuilder();
        this.propToText = new HashMap<>();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(hits);sb.append(" hits.");
        sb.append(newline);
        sb.append(multipleProperties);sb.append(" multiple properties.");
        sb.append(newline);
        sb.append(this.propToText.toString());
        sb.append(newline);
        sb.append(this.logs.toString());
        return sb.toString();
    }

    public int getHits() {
        return this.hits;
    }
}
