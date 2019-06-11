package avroschema.util;

import avroschema.linked.*;
import de.uni_mannheim.constant.CHARACTER;
import de.uni_mannheim.constant.NE_TYPE;
import de.uni_mannheim.constant.POS_TAG;
import de.uni_mannheim.constant.SEPARATOR;
import de.uni_mannheim.utils.coreNLP.WordUtils;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.HashMap;
import java.util.List;

public class TripleLinkedUtils {
    public static String relToLemmatizedString(TripleLinked triple) {
        StringBuilder sb = new StringBuilder();

        for (TokenLinked token: triple.getRelation()) {
            if (token == null) {
                sb.append(token.getWord());
            } else {
                sb.append(token.getLemma());
            }
            sb.append(CHARACTER.SPACE);
        }

        return sb.toString().trim();
    }

    public static String subjToLemmatizedString(TripleLinked triple) {
        StringBuilder sb = new StringBuilder();

        for (TokenLinked token: triple.getSubject()) {
            if (token.getLemma() == null) {
                sb.append(token.getWord());
            } else {
                sb.append(token.getLemma());
            }
            sb.append(CHARACTER.SPACE);
        }

        return sb.toString().trim();
    }

    public static String objToLemmatizedString(TripleLinked triple) {
        StringBuilder sb = new StringBuilder();

        if (triple.getObject() == null || triple.getObject().isEmpty()) {
            return "";
        }

        for (TokenLinked token: triple.getObject()) {
            if (token.getLemma() == null) {
                sb.append(token.getWord());
            } else {
                sb.append(token.getLemma());
            }
            sb.append(CHARACTER.SPACE);
        }

        return sb.toString().trim();
    }

    public static String getSubjWords(TripleLinked triple) {
        return TokenLinkedUtils.linkedTokensWordsToString(triple.getSubject());
    }

    public static String getObjWords(TripleLinked triple) {
        return TokenLinkedUtils.linkedTokensWordsToString(triple.getObject());
    }

    public static String getRelWords(TripleLinked triple) {
        return TokenLinkedUtils.linkedTokensWordsToString(triple.getRelation());
    }

    public static String getSubjLink(TripleLinked triple) {
        return getLink(triple.getSubject());
    }

    public static String getObjLink(TripleLinked triple) {
        return getLink(triple.getObject());
    }

    /**
     * Given a list of tokens, return the link they contain. If all of the tokens contain one same link - return that
     * link. If at least one token contains different link from the first one, return empty link
     * @param tokens: list of linked tokens
     * @return string (link)
     */
    private static String getLink(List<TokenLinked> tokens) {
        if (tokens.size() == 0 || tokens == null) {
            return "";
        }

        String firstLink = tokens.get(0).getWLink().getWikiLink();
        boolean isSameLink = true;
        for (TokenLinked t: tokens) {
            if (!t.getWLink().getWikiLink().equals(firstLink)) {
                isSameLink = false;
            }
        }
        if (isSameLink) {
            return firstLink;
        } else {
            return "";
        }
    }

    public static String getDisambiguationSubj(TripleLinked triple) {
        return getDisambiguation(triple.getSubject());
    }

    public static String getDisambiguationObj(TripleLinked triple) {
        return getDisambiguation(triple.getObject());
    }

    private static String getDisambiguation(List<TokenLinked> tokens) {
        if (tokens.size() == 0 || tokens == null) {
            return "";
        }
        if (tokens.size() == 1 && tokens.get(0).getPos().equals(POS_TAG.NNS)) {
            if (tokens.get(0).getLemma() != null) {
                return tokens.get(0).getLemma();
            } else {
                return tokens.get(0).getWord();
            }
        }
        if (tokens.size() == 2 && tokens.get(0).getNer().equals(NE_TYPE.QUANTITY)) {
            if (tokens.get(1).getPos().equals(POS_TAG.NNS)) {
                if (tokens.get(1).getLemma() != null) {
                    return tokens.get(1).getLemma();
                } else {
                    return tokens.get(1).getWord();
                }
            } else {
                return tokens.get(1).getWord();
            }
        }
        return TokenLinkedUtils.linkedTokensWordsToString(tokens);
    }

    /**
     * @param triple linked triple
     * @return true -> if the whole subj and the whole obj are one same link; false -> otherwise or if the object is empty
     */
    public static boolean argumentsAreSameLink(TripleLinked triple) {
        if (triple.getObject().size() == 0) {
            return false;
        }
        if (TokenLinkedUtils.allTokensAreOneLink(triple.getSubject()) && TokenLinkedUtils.allTokensAreOneLink(triple.getObject())) {
            if (TokenLinkedUtils.haveSameLink(triple.getSubject().get(0), triple.getObject().get(0))) {
                return true;
            }
        }
        return false;
    }

    public static boolean hasBeRelation(TripleLinked triple) {
        if (triple.getRelation().size() == 1) {
            TokenLinked relWord = triple.getRelation().get(0);
            if (relWord.getLemma() != null) {
                if (relWord.getLemma().toLowerCase().equals("be")) {
                    return true;
                }
            } else {
                if (relWord.getWord().equalsIgnoreCase("is") || relWord.getWord().equalsIgnoreCase("was") ||
                        relWord.getWord().equalsIgnoreCase("be") || relWord.getWord().equalsIgnoreCase("were") ||
                        relWord.getWord().equalsIgnoreCase("are")) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * If all tokens are one link in subj/obj (or a quantity modifying an argument, then write that link. Otherwise, write an empty string.
     * @param triple: linked triple
     * @return links about subj/obj
     */
    public static String linksToString(TripleLinked triple) {
        StringBuilder sbResult = new StringBuilder();

        sbResult.append("SubjLink:[");
        if (TokenLinkedUtils.allTokensAreOneLink(triple.getSubject())) {
            sbResult.append(triple.getSubject().get(0).getWLink().getWikiLink());
        } else if (TokenLinkedUtils.allTokensAreQuantityWithOneLink(triple.getSubject())) {
            for (TokenLinked token: triple.getSubject()) {
                if (!token.getWLink().getWikiLink().equals("")) {
                    sbResult.append(token.getWLink().getWikiLink());
                    break;
                }
            }
        }
        sbResult.append("]");

        sbResult.append("\tObjLink:[");
        if (TokenLinkedUtils.allTokensAreOneLink(triple.getObject())) {
            sbResult.append(triple.getObject().get(0).getWLink().getWikiLink());
        } else if (TokenLinkedUtils.allTokensAreQuantityWithOneLink(triple.getObject())) {
            for (TokenLinked token: triple.getObject()) {
                if (!token.getWLink().getWikiLink().equals("")) {
                    sbResult.append(token.getWLink().getWikiLink());
                    break;
                }
            }
        }
        sbResult.append("]");

        return sbResult.toString();
    }

    public static String annotationsToString(TripleLinked triple) {
        StringBuilder result = new StringBuilder();

        // Factuality
        result.append("factuality: (");
        result.append(factualityToString(triple));
        result.append(CHARACTER.TAB);

        // Quantity
        result.append("quantities: (");
        result.append(quantitiesToString(triple));
        result.append(CHARACTER.RPARENTHESIS);
        result.append(CHARACTER.TAB);

        // Attribution
        result.append("attribution: (");
        result.append(attributionToString(triple));
        result.append(CHARACTER.RPARENTHESIS);
        result.append(CHARACTER.TAB);

        // Time and space
        result.append("time: (");
        result.append(timeToString(triple));
        result.append(CHARACTER.RPARENTHESIS);
        result.append(CHARACTER.TAB);

        result.append("space: (");
        result.append(spaceToString(triple));
        result.append(CHARACTER.RPARENTHESIS);

        return result.toString();
    }

    public static String factualityToString(TripleLinked triple) {
        StringBuilder result = new StringBuilder();

        // Factuality
        result.append(triple.getPolarity());
        result.append(CHARACTER.COMMA);
        result.append(triple.getModality());
        result.append(CHARACTER.RPARENTHESIS);

        return result.toString().trim();
    }

    public static String quantitiesToString(TripleLinked triple) {
        StringBuilder result = new StringBuilder();

        for (String key: triple.getQuantities().keySet()) {
            result.append(key);
            result.append(CHARACTER.COMMA);
            result.append(triple.getQuantities().get(key));
            result.append(CHARACTER.SPACE);
        }

        return result.toString().trim();
    }

    public static String canonicalLinksToString(TripleLinked triple) {
        StringBuilder result = new StringBuilder();

        for (String key: triple.getCanonicalLinks().keySet()) {
            result.append(key);
            result.append(CHARACTER.COMMA);
            result.append(triple.getCanonicalLinks().get(key));
            result.append(CHARACTER.SPACE);
        }

        return result.toString().trim();
    }

    public static String attributionToString(TripleLinked triple) {
        StringBuilder result = new StringBuilder();

        if (triple.getAttributionLinked() == null) {
            return "NO ATTRIBUTION DETECTED";
        } else {
            result.append(triple.getAttributionLinked().getAttributionPhraseOriginal());
            result.append(CHARACTER.COMMA);
            result.append(" Predicate: ");
            result.append(triple.getAttributionLinked().getPredicate());
            result.append(", Factuality:(");
            result.append(triple.getAttributionLinked().getPolarity());
            result.append(CHARACTER.COMMA);
            result.append(triple.getAttributionLinked().getModality());
            result.append(CHARACTER.RPARENTHESIS);
        }

        return result.toString().trim();
    }

    public static String timeToString(TripleLinked triple) {
        StringBuilder result = new StringBuilder();

        if (triple.getTimeLinked() == null || triple.getTimeLinked().isEmpty()) {
            result.append(CHARACTER.EMPTY_STRING);
        } else {
            for (TimeLinked time: triple.getTimeLinked()) {
                if (time.getPredicate() != null) {
                    result.append("pred=");
                    result.append(time.getPredicate().getWord());
                    result.append(CHARACTER.COMMA);
                    result.append(CHARACTER.SPACE);
                }

                for (TokenLinked token: time.getCoreWords()) {
                    result.append(token.getWord());
                    result.append(CHARACTER.SPACE);
                }

                result.append(", res:");
                result.append(time.getDisambiguated());

                if (time.getPreMods() != null || !time.getPreMods().isEmpty()) {
                    result.append(", premods: ");
                    for (TokenLinked token: time.getPreMods()) {
                        result.append(token.getWord());
                        result.append(CHARACTER.SPACE);
                    }
                }

                if (time.getPostMods() != null || !time.getPostMods().isEmpty()) {
                    result.append(", postmods: ");
                    for (TokenLinked token: time.getPostMods()) {
                        result.append(token.getWord());
                        result.append(CHARACTER.SPACE);
                    }
                }

                result.append(CHARACTER.SEMI_COLON);
                result.append(CHARACTER.SPACE);
            }
        }

        // Subject time
        if (triple.getSubjTimeLinked().getModifiedWord() != null) {
            result.append("SubjTime:[");
            result.append(triple.getSubjTimeLinked().getModifiedWord());
            result.append(CHARACTER.COMMA);
            for (TokenLinked token: triple.getSubjTimeLinked().getTimeLinked().getAllWords()) {
                result.append(token.getWord());
                result.append(CHARACTER.SPACE);
            }
            result.append("]");
        }

        // Object time
        if (triple.getObjTimeLinked().getModifiedWord() != null) {
            result.append("ObjTime:[");
            result.append(triple.getObjTimeLinked().getModifiedWord());
            result.append(CHARACTER.COMMA);
            for (TokenLinked token: triple.getObjTimeLinked().getTimeLinked().getAllWords()) {
                result.append(token.getWord());
                result.append(CHARACTER.SPACE);
            }
            result.append("]");
        }

        return result.toString().trim();
    }

    public static String spaceToString(TripleLinked triple) {
        StringBuilder result = new StringBuilder();

        if (triple.getSpaceLinked() == null || triple.getSpaceLinked().isEmpty()) {
            result.append(CHARACTER.EMPTY_STRING);
        } else {
            for (SpaceLinked space: triple.getSpaceLinked()) {
                if (space.getPredicate() != null) {
                    result.append("pred=");
                    result.append(space.getPredicate().getWord());
                    result.append(CHARACTER.COMMA);
                    result.append(CHARACTER.SPACE);
                }

                for (TokenLinked token: space.getCoreWords()) {
                    result.append(token.getWord());
                    result.append(CHARACTER.SPACE);
                }

                if (space.getPreMods() != null || !space.getPreMods().isEmpty()) {
                    result.append(", premods: ");
                    for (TokenLinked token: space.getPreMods()) {
                        result.append(token.getWord());
                        result.append(CHARACTER.SPACE);
                    }
                }

                if (space.getPostMods() != null || !space.getPostMods().isEmpty()) {
                    result.append(", postmods: ");
                    for (TokenLinked token: space.getPostMods()) {
                        result.append(token.getWord());
                        result.append(CHARACTER.SPACE);
                    }
                }

                result.append(CHARACTER.SEMI_COLON);
                result.append(CHARACTER.SPACE);
            }
        }

        // Subject space
        if (triple.getSubjSpaceLinked().getModifiedWord() != null) {
            result.append("SubjSpace:[");
            result.append(triple.getSubjSpaceLinked().getModifiedWord());
            result.append(CHARACTER.COMMA);
            for (TokenLinked token: triple.getSubjSpaceLinked().getSpace().getAllWords()) {
                result.append(token.getWord());
                result.append(CHARACTER.SPACE);
            }
            result.append("]");
        }

        // Object space
        if (triple.getObjSpaceLinked().getModifiedWord() != null) {
            result.append("ObjSpace:[");
            result.append(triple.getObjSpaceLinked().getModifiedWord());
            result.append(CHARACTER.COMMA);
            for (TokenLinked token: triple.getObjSpaceLinked().getSpace().getAllWords()) {
                result.append(token.getWord());
                result.append(CHARACTER.SPACE);
            }
            result.append("]");
        }


        return result.toString();
    }

    /**
     * Given a linked triple, return a triple in the form of string (including the original sentence, the original words
     * of the SPO, the semantic annotations and the links of the arguments if found)
     * @param triple input linked triple
     * @return string representation of the triple
     */
    public static String tripleLinkedToString(TripleLinked triple) {
        StringBuilder sbResult = new StringBuilder();

        // Sentence
        sbResult.append("Sentence: ");
        sbResult.append(SentenceLinkedUtil.getSentenceLinkedString(triple.getSentenceLinked()));
        sbResult.append(CHARACTER.TAB);

        // Triple
        sbResult.append("Triple: ");
        sbResult.append(CHARACTER.QUOTATION_MARK);
        sbResult.append(TokenLinkedUtils.linkedTokensWordsToString(triple.getSubject()));
        sbResult.append(CHARACTER.QUOTATION_MARK);
        sbResult.append(SEPARATOR.TAB);
        sbResult.append(CHARACTER.QUOTATION_MARK);
        sbResult.append(TokenLinkedUtils.linkedTokensWordsToString(triple.getRelation()));
        sbResult.append(CHARACTER.QUOTATION_MARK);
        sbResult.append(SEPARATOR.TAB);
        sbResult.append(CHARACTER.QUOTATION_MARK);
        sbResult.append(TokenLinkedUtils.linkedTokensWordsToString(triple.getObject()));
        sbResult.append(CHARACTER.QUOTATION_MARK);

        // Annotations and links
        sbResult.append(CHARACTER.TAB);
        sbResult.append("Conf: ");
        sbResult.append(triple.getConfidenceScore());
        sbResult.append(CHARACTER.TAB);
        sbResult.append(TripleLinkedUtils.annotationsToString(triple));

        sbResult.append(CHARACTER.TAB);
        sbResult.append("Links: (");
        sbResult.append(TripleLinkedUtils.linksToString(triple));
        sbResult.append(")");

        sbResult.append(CHARACTER.TAB);
        sbResult.append("Canonical links:(");
        sbResult.append(canonicalLinksToString(triple));
        sbResult.append(")");

        return sbResult.toString().trim();
    }

    /**
     * Given a triple and a redirects-map, return a hash map where the key is the original link and the value is the
     * corresponding redirect link from Wikipedia
     *
     * @param triple: input triple
     * @param redirectsMap: the redirects map from Wikipedia
     * @return the final redirect links map
     */
    public static HashMap<String, String> getCanonicalLinksMap(TripleLinked triple, HashMap<String, String> redirectsMap) {
        ObjectOpenHashSet<String> links = new ObjectOpenHashSet<>();
        links.addAll(TokenLinkedUtils.getUniqueLinks(triple.getSubject()));
        links.addAll(TokenLinkedUtils.getUniqueLinks(triple.getRelation()));
        links.addAll(TokenLinkedUtils.getUniqueLinks(triple.getObject()));

        HashMap<String, String> canonicalLinks = new HashMap<>();
        if (!links.isEmpty()) {
            for (String link: links) {
                if (redirectsMap.containsKey(link)) {
                    canonicalLinks.put(link, redirectsMap.get(link));
                }
            }
        }

        return canonicalLinks;
    }
}
