package avroschema.util;

import avroschema.linked.TokenLinked;
import avroschema.linked.WikiLink;

import de.uni_mannheim.constant.CHARACTER;
import de.uni_mannheim.constant.NE_TYPE;
import de.uni_mannheim.constant.POS_TAG;

import edu.stanford.nlp.ling.IndexedWord;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.List;

/**
 * @author Kiril Gashteovski
 */
public class TokenLinkedUtils {
    /**
     * Given an IndexedWord object, return TokenLinked (empty link assumed)
     * @param w IndexedWord object
     * @return TokenLinked object (with empty link)
     */
    public static TokenLinked toTokenLinked(IndexedWord w) {
        TokenLinked tl = new TokenLinked();
        avroschema.linked.Span span = new avroschema.linked.Span();
        span.setStartIndex(w.beginPosition());
        span.setEndIndex(w.endPosition());
        tl.setSpan(span);
        tl.setWord(w.word());
        tl.setPos(w.tag());
        tl.setNer(w.ner());
        tl.setLemma(w.lemma());
        tl.setIndex(w.index());
        WikiLink wLink = new WikiLink();
        wLink.setOffsetBeginInd(-1);
        wLink.setOffsetEndInd(-1);
        wLink.setPhrase(CHARACTER.EMPTY_STRING);
        wLink.setWikiLink(CHARACTER.EMPTY_STRING);
        tl.setWLink(wLink);
        return tl;
    }

    /**
     * @param tokens list of linked tokens
     * @return string consisted of the lemmas, separated by space
     */
    public static String linkedTokensLemmasToString(List<TokenLinked> tokens) {
        StringBuilder sb = new StringBuilder();
        for (TokenLinked t: tokens) {
            if (t == null) {
                sb.append(t.getWord());
            } else {
                sb.append(t.getLemma());
            }
            sb.append(CHARACTER.SPACE);
        }
        return sb.toString().trim();
    }

    /**
     * @param tokens list of linked tokens
     * @return string consisted of the words, separated by space
     */
    public static String linkedTokensWordsToString(List<TokenLinked> tokens) {
        if (tokens.size() == 0 || tokens == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (TokenLinked t: tokens) {
            sb.append(t.getWord());
            sb.append(CHARACTER.SPACE);
        }
        return sb.toString().trim();
    }

    /**
     * @param tokens list of linked tokens
     * @return true -> if it's one PRP/PRP$; false -> otherwise
     */
    public static boolean isPRP(List<TokenLinked> tokens) {
        if (tokens.size() == 1) {
            if (tokens.get(0).getPos().equals(POS_TAG.PRP) || tokens.get(0).getPos().equals(POS_TAG.PRP_P)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param tokens list of linked tokens
     * @return true -> if it's a DT; false -> otherwise
     */
    public static boolean isDT(List<TokenLinked> tokens) {
        if (tokens.size() == 1) {
            if (tokens.get(0).getPos().equals(POS_TAG.DT)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param tokens list of linked tokens
     * @return true -> if it's a WP or WP$; false -> otherwise
     */
    public static boolean isWP(List<TokenLinked> tokens) {
        if (tokens.size() == 1) {
            if (tokens.get(0).getPos().equals(POS_TAG.WP) || tokens.get(0).getPos().equals(POS_TAG.WP_P)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if within the tokens there are NERs modifying a quantity (N1 N2 ... Nn QUANTITY)
     * or quantity modifying NER (QUANTITY N1 N2 ... Nn)
     * @param tokens list of tokens
     * @return true if the tokens are quantity modifying NER or NER modifying a quantity
     */
    public static boolean allTokensAreQuantityWithOneNER(List<TokenLinked> tokens) {
        // Conditions for "false"
        if (tokens.size() <= 1) {
            return false;
        }
        boolean isFirstTokenQuantity = tokens.get(0).getNer().equals(NE_TYPE.QUANTITY);
        boolean isLastTokenQuantity = tokens.get(tokens.size() - 1).getNer().equals(NE_TYPE.QUANTITY);
        if (!isFirstTokenQuantity && !isLastTokenQuantity) {
            return false;
        }

        // Check if the tokens are "QUANTITY L1 L2 ... Ln"
        List<TokenLinked> sublist = tokens.subList(1, tokens.size());
        if (isFirstTokenQuantity && allTokensAreOneNER(sublist)) {
            return true;
        }

        // Checks if the tokens are "L1 L2 ... Ln QUANTITY"
        sublist = tokens.subList(0, tokens.size() - 1);
        if (isLastTokenQuantity && allTokensAreOneNER(sublist)) {
            return true;
        }

        return false;
    }

    /**
     * Given a list of linked tokens, checks if they all are one NER
     * @param tokens list of linked tokens
     * @return true -> if all of them form one NER; false -> otherwise
     */
    public static boolean allTokensAreOneNER(List<TokenLinked> tokens) {
        if (tokens.size() == 0) {
            return false;
        }

        String firstNER = tokens.get(0).getNer();
        if (firstNER.equals(NE_TYPE.NO_NER)) {
            return false;
        }
        for (TokenLinked t: tokens) {
            if (!t.getNer().equals(firstNER)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Given a list of linked tokens, checks if they all contain the same link
     * @param tokens list of linked tokens
     * @return true -> if all of them contain the same link; false -> otherwise
     */
    public static boolean allTokensAreOneLink(List<TokenLinked> tokens) {
        if (tokens.size() == 0) {
            return false;
        }

        String firstLink = tokens.get(0).getWLink().getWikiLink();
        if (firstLink.equals("")) {
            return false;
        }
        for (TokenLinked t: tokens) {
            if (!t.getWLink().getWikiLink().equals(firstLink)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if within the tokens there are links modifying a quantity (L1 L2 ... Ln QUANTITY)
     * or quantity modifying links (QUANTITY L1 L2 ... Ln)
     * @param tokens list of tokens
     * @return true if the tokens are quantity modifying link or link modifying a quantity
     */
    public static boolean allTokensAreQuantityWithOneLink(List<TokenLinked> tokens) {
        // Conditions for "false"
        if (tokens.size() <= 1) {
            return false;
        }
        boolean isFirstTokenQuantity = tokens.get(0).getNer().equals("QUANTITY");
        boolean isLastTokenQuantity = tokens.get(tokens.size() - 1).getNer().equals("QUANTITY");
        if (!isFirstTokenQuantity && !isLastTokenQuantity) {
            return false;
        }

        // Check if the tokens are "QUANTITY L1 L2 ... Ln"
        List<TokenLinked> sublist = tokens.subList(1, tokens.size());
        if (isFirstTokenQuantity && allTokensAreOneLink(sublist)) {
            return true;
        }

        // Checks if the tokens are "L1 L2 ... Ln QUANTITY"
        sublist = tokens.subList(0, tokens.size() - 1);
        if (isLastTokenQuantity && allTokensAreOneLink(sublist)) {
            return true;
        }

        return false;
    }

    /**
     * Assumption: list of tokens is a quantity with NER
     * @param tokens: list of tokens which represent quantity with NER
     * @return the NER type, not QUANTITY as NER
     */
    private static String getNERFromQuantityWithNER(List<TokenLinked> tokens) {
        String objNER = NE_TYPE.NO_NER;

        for (TokenLinked token: tokens) {
            if (!token.getNer().equals(NE_TYPE.QUANTITY)) {
                objNER = token.getNer();
                break;
            }
        }

        return objNER;
    }

    /**
     * For a list of tokens, return the NER of the whole phrase. If there is a quantity modifying a NER, return the
     * value of the NER. If all of them are one entity type - return that type. Otherwise, return "no NER type" value
     * @param tokens list of tokens
     * @return NER type of the list of tokens (see documentation)
     */
    public static String getNERIgnoringQuantities(List<TokenLinked> tokens) {
        String ner = NE_TYPE.NO_NER;
        boolean isOneNER = TokenLinkedUtils.allTokensAreOneNER(tokens);
        boolean isQuantityWithOneNER = TokenLinkedUtils.allTokensAreQuantityWithOneNER(tokens);

        if (isOneNER) {
            return tokens.get(0).getNer();
        } else if (isQuantityWithOneNER) {
            ner = getNERFromQuantityWithNER(tokens);
        }

        return ner;
    }

    /**
     * Checks if a token is temporal (having NER to be either TIME, DATE, DURATION or SET)
     * @param token input linked token
     * @return true if the NER is either TIME, DATE, DURATION or SET
     */
    public static boolean tokenIsTemporal(TokenLinked token) {
        if (token.getNer().equals(NE_TYPE.TIME)) {
            return true;
        }
        if (token.getNer().equals(NE_TYPE.DATE)) {
            return true;
        }
        if (token.getNer().equals(NE_TYPE.DURATION)) {
            return true;
        }
        if (token.getNer().equals(NE_TYPE.SET)) {
            return true;
        }
        return false;
    }

    /**
     * Checks if a token is spatial (having NER to be LOCATION)
     * @param token input linked token
     * @return true if the NER is LOCATION
     */
    public static boolean tokenIsSpatial(TokenLinked token) {
        return token.getNer().equals(NE_TYPE.LOCATION);
    }

    /**
     * @param t linked token
     * @return true -> if the token contains a link; false -> otherwise
     */
    public static boolean hasLink(TokenLinked t) {
        return !t.getWLink().getWikiLink().equals("");
    }

    /**
     * @param t1 linked token 1
     * @param t2 linked token 2
     * @return true -> if the link in t1 is the same as the one in t2
     */
    public static boolean haveSameLink(TokenLinked t1, TokenLinked t2) {
        return t1.getWLink().getWikiLink().equals(t2.getWLink().getWikiLink());
    }

    public static ObjectOpenHashSet<String> getUniqueLinks(List<TokenLinked> tokens) {
        ObjectOpenHashSet<String> uniqueLinks = new ObjectOpenHashSet<>();
        for (TokenLinked t: tokens) {
            if (!t.getWLink().getWikiLink().equals("")) {
                uniqueLinks.add(t.getWLink().getWikiLink());
            }
        }
        return uniqueLinks;
    }
}
