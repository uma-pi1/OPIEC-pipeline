package de.uni_mannheim.querykb;

import avroschema.linked.TokenLinked;
import avroschema.linked.TripleLinked;
import de.uni_mannheim.constant.NE_TYPE;
import de.uni_mannheim.querykb.query.offline.RelsBetweenWikiPages;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Sven Hertling
 * @author Kiril Gashteovski
 */

public class KBHitsCounting {
    private static final String ONTO_PLACE_HOLDER = "/ontology/";
    private static final String PROP_PLACE_HOLDER = "/property/";
    private static final String YAGO_PLACE_HOLDER = "yago-knowledge";

    private RelsBetweenWikiPages KBQuery;
    private Set<String> KBRels = new HashSet<>();
    private int reflexiveTriple;
    private int tripleCount;
    private int triplehits;
    private int multiplehits;
    private int ontohits;
    private int multipleOntoHits;
    private int oneOntoHit;
    private int prophits;
    private int multiplePropHits;
    private int onePropHit;
    private int erroneousLink;
    private int reverseHits;
    private int reverseOrStraightHits;
    private int reverseAndStraightHits;
    private int reverseHitsOnly;

    public KBHitsCounting(RelsBetweenWikiPages query) {
        this.KBQuery = query;
        this.KBRels = new HashSet<>();
        this.reflexiveTriple = 0;
        this.tripleCount = 0;
        this.triplehits = 0;
        this.multiplehits = 0;
        this.ontohits = 0;
        this.multipleOntoHits = 0;
        this.oneOntoHit = 0;
        this.prophits = 0;
        this.multiplePropHits = 0;
        this.onePropHit = 0;
        this.erroneousLink = 0;
        this.reverseHits = 0;
        this.reverseAndStraightHits = 0;
        this.reverseOrStraightHits = 0;
        this.reverseHitsOnly = 0;
    }

    public KBHitsCounting() {
        this.KBQuery = new RelsBetweenWikiPages();
        this.KBRels = new HashSet<>();
        this.reflexiveTriple = 0;
        this.tripleCount = 0;
        this.triplehits = 0;
        this.multiplehits = 0;
        this.ontohits = 0;
        this.multipleOntoHits = 0;
        this.oneOntoHit = 0;
        this.prophits = 0;
        this.multiplePropHits = 0;
        this.onePropHit = 0;
        this.erroneousLink = 0;
        this.reverseHits = 0;
        this.reverseAndStraightHits = 0;
        this.reverseOrStraightHits = 0;
        this.reverseHitsOnly = 0;
    }

    public KBHitsCounting(KBHitsCounting kbHitsCounting) {
        this.KBQuery = kbHitsCounting.getKBQuery();
        this.KBRels = kbHitsCounting.getKBRels();
        this.reflexiveTriple = kbHitsCounting.getReflexiveTriple();
        this.tripleCount = kbHitsCounting.getTripleCount();
        this.triplehits = kbHitsCounting.getTriplehits();
        this.multiplehits = kbHitsCounting.getMultiplehits();
        this.ontohits = kbHitsCounting.getOntohits();
        this.multipleOntoHits = kbHitsCounting.getMultipleOntoHits();
        this.oneOntoHit = kbHitsCounting.getOneOntoHit();
        this.prophits = kbHitsCounting.getProphits();
        this.multiplePropHits = kbHitsCounting.getMultiplePropHits();
        this.onePropHit = kbHitsCounting.getOnePropHit();
        this.erroneousLink = kbHitsCounting.getErroneousLink();
        this.reverseHits = kbHitsCounting.getReverseHits();
        this.reverseAndStraightHits = kbHitsCounting.getReverseAndStraightHits();
        this.reverseOrStraightHits = kbHitsCounting.getReverseOrStraightHits();
        this.reverseHitsOnly = kbHitsCounting.getReverseHitsOnly();
    }

    public void updateCounters(TripleLinked triple) {
        Set<String> properties;
        Set<String> reverseProps;
        this.tripleCount++;

        String subjectLink = getPageTitle(triple.getSubject());
        String objectLink = getPageTitle(triple.getObject());

        if (subjectLink.equals("") || objectLink.equals("")) {
            this.erroneousLink++;
            //this.tripleCount--;
            return;
        }

        //TODO: iterate only over "unique" triples
        if (subjectLink.equals(objectLink)) {
            this.reflexiveTriple++;
            return;
        }

        properties = KBQuery.getConnectingPropertiesGivenPageTitle(subjectLink, objectLink);

        int tempOntoHits = 0;
        int tempPropHits = 0;

        if (properties.size() > 0) {
            this.triplehits++;
            if (properties.size() > 1) {
                this.multiplehits++;
            }

            // For each property, count the ontology and property ones
            for (String prop: properties) {
                if (prop.contains(ONTO_PLACE_HOLDER) || prop.contains(YAGO_PLACE_HOLDER)) {
                    tempOntoHits++;
                } else if (prop.contains(PROP_PLACE_HOLDER)) {
                    tempPropHits++;
                }
                this.KBRels.add(prop);
            }

            // Count ontology hits
            if (tempOntoHits > 0) {
                this.ontohits++;
                if (tempOntoHits == 1) {
                    this.oneOntoHit++;
                } else if (tempOntoHits > 1) {
                    this.multipleOntoHits++;
                }
            }

            // Count property hits
            if (tempPropHits > 0) {
                this.prophits++;
                if (tempPropHits == 1) {
                    this.onePropHit++;
                } else if (tempPropHits > 1) {
                    this.multiplePropHits++;
                }
            }
        }

        // See reverse hits (onto hits only for DBPedia)
        int tempReverseOntoHits = 0;
        int tempReverseHitsOnly = 0;
        int tempReverseAndStraightHits = 0;

        reverseProps = this.KBQuery.getConnectingPropertiesGivenPageTitle(objectLink, subjectLink);
        if (reverseProps.size() > 0) {
            for (String prop: reverseProps) {
                if (prop.contains(ONTO_PLACE_HOLDER) || prop.contains(YAGO_PLACE_HOLDER)) {
                    tempReverseOntoHits++;
                    if (tempOntoHits == 0) {
                        tempReverseHitsOnly++;
                    }
                    if (properties.size() > 0) {
                        if (KBQueryUtils.containsOntoRels(properties) || KBQueryUtils.containsYAGORel(properties)) {
                            tempReverseAndStraightHits++;
                        }
                    }
                }
            }
        }

        if (tempReverseOntoHits > 0) {
            this.reverseHits++;
        }
        if (tempReverseHitsOnly > 0) {
            this.reverseHitsOnly++;
        }
        if (tempReverseAndStraightHits > 0) {
            this.reverseAndStraightHits++;
        }

        // Count reverse or straight hits (onto hits only for DBPedia)
        if (reverseProps.size() > 0 || properties.size() > 0) {
            boolean propsContainOntoRel = KBQueryUtils.containsOntoRels(properties) || KBQueryUtils.containsYAGORel(properties);
            boolean reversePropsContainOntoRel = KBQueryUtils.containsOntoRels(reverseProps) || KBQueryUtils.containsYAGORel(properties);

            if (propsContainOntoRel || reversePropsContainOntoRel) {
                this.reverseOrStraightHits++;
            }
        }
    }

    public void printCounters() {
        System.out.println("# triples: " + this.tripleCount);
        //System.out.println("# unique triples: NaN"); // TODO
        System.out.println("# total hits: " + this.triplehits);
        System.out.println("# hits (onto): " + this.ontohits);
        System.out.println("\t# onto hits with 1 rel:  " + this.oneOntoHit);
        System.out.println("# hits (properties)" + this.prophits);
        System.out.println("\t# prop hits with 1 rel: " + this.onePropHit);
        System.out.println("# reverse hits: " + this.reverseHits);
        System.out.println("# reverse OR straight hits: " + this.reverseOrStraightHits); //TODO
        System.out.println("# reverse AND straight hits: " + this.reverseAndStraightHits); // TODO
        System.out.println("# reverse hits only: " + this.reverseHitsOnly); // TODO
    }

    public static String getPageTitle(List<TokenLinked> tokenLinkedList){
        if (tokenLinkedList.get(0).getNer().equals(NE_TYPE.QUANTITY)) {
            if (tokenLinkedList.size() == 1) {
                return makeUppercasedPageTitle(tokenLinkedList.get(0).getWLink().getWikiLink());
            } else {
                return makeUppercasedPageTitle(tokenLinkedList.get(1).getWLink().getWikiLink());
            }
        } else {
            return makeUppercasedPageTitle(tokenLinkedList.get(0).getWLink().getWikiLink());
        }
    }

    public static String makeUppercasedPageTitle(String input) {
        if (input.equals("")) {
            return "";
        }

        String uppercased = input.substring(0, 1).toUpperCase() + input.substring(1);
        String[] mwe = uppercased.split(" ");
        if (mwe.length > 1) {
            uppercased = String.join("_", mwe);
        }
        int lastAnchor = uppercased.lastIndexOf("#");
        if(lastAnchor != -1){
            uppercased = uppercased.substring(0, lastAnchor);
        }
        return uppercased;
    }

    // Getters
    public RelsBetweenWikiPages getKBQuery() {
        return KBQuery;
    }

    public Set<String> getKBRels() {
        return KBRels;
    }

    public int getReflexiveTriple() {
        return reflexiveTriple;
    }

    public int getTripleCount() {
        return tripleCount;
    }

    public int getTriplehits() {
        return triplehits;
    }

    public int getMultiplehits() {
        return multiplehits;
    }

    public int getOntohits() {
        return ontohits;
    }

    public int getMultipleOntoHits() {
        return multipleOntoHits;
    }

    public int getOneOntoHit() {
        return oneOntoHit;
    }

    public int getProphits() {
        return prophits;
    }

    public int getMultiplePropHits() {
        return multiplePropHits;
    }

    public int getOnePropHit() {
        return onePropHit;
    }

    public int getErroneousLink() {
        return erroneousLink;
    }

    public int getReverseHits() {
        return reverseHits;
    }

    public int getReverseOrStraightHits() {
        return reverseOrStraightHits;
    }

    public int getReverseAndStraightHits() {
        return reverseAndStraightHits;
    }

    public int getReverseHitsOnly() {
        return reverseHitsOnly;
    }

    // Setters
    public void setTriplehits(int triplehits) {
        this.triplehits = triplehits;
    }

    public void setOntohits(int ontohits) {
        this.ontohits = ontohits;
    }

    public void setMultipleOntoHits(int multipleOntoHits) {
        this.multipleOntoHits = multipleOntoHits;
    }

    public void setOneOntoHit(int oneOntoHit) {
        this.oneOntoHit = oneOntoHit;
    }

    public void setProphits(int prophits) {
        this.prophits = prophits;
    }

    public void setOnePropHit(int onePropHit) {
        this.onePropHit = onePropHit;
    }

    public void setReverseHits(int reverseHits) {
        this.reverseHits = reverseHits;
    }

    public void setReverseOrStraightHits(int reverseOrStraightHits) {
        this.reverseOrStraightHits = reverseOrStraightHits;
    }

    public void setReverseAndStraightHits(int reverseAndStraightHits) {
        this.reverseAndStraightHits = reverseAndStraightHits;
    }

    public void setReverseHitsOnly(int reverseHitsOnly) {
        this.reverseHitsOnly = reverseHitsOnly;
    }
}
