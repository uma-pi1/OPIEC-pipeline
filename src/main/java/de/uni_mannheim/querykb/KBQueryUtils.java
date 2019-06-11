package de.uni_mannheim.querykb;

import java.util.Set;

/**
 * @author Kiril Gashteovski
 */

public class KBQueryUtils {
    private static final String ONTO_PLACE_HOLDER = "/ontology/";
    private static final String YAGO_PLACE_HOLDER = "yago-knowledge";

    /**
     * Given a set of KB properties, check if there's a DBPedia onto relation
     * @param properties: set of KB relations (properties)
     * @return true -> if there's a DBPedia onto relation; false -> otherwise
     */
    public static boolean containsOntoRels(Set<String> properties) {
        for (String prop: properties) {
            if (prop.contains(ONTO_PLACE_HOLDER)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param properties: set of KB relations (properties)
     * @return true -> if there's a YAGO rel; false -> otherwise
     */
    public static boolean containsYAGORel(Set<String> properties) {
        for (String prop: properties) {
            if (prop.contains(YAGO_PLACE_HOLDER)) {
                return true;
            }
        }
        return false;
    }
}
