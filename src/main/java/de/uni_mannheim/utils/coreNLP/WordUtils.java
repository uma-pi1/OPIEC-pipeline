package de.uni_mannheim.utils.coreNLP;

import de.uni_mannheim.constant.POS_TAG;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.IndexedWord;

/**
 * @author Kiril Gashteovski
 */
public class WordUtils {
    /**
     * Checks if a word is some kind of a verb (i.e. if it has POS tag: VB, VBD, VBG, VBN, VBP or VBZ)
     * @param w: indexed word object
     * @return true if it is a verb, false otherwise
     */
    public static boolean isVerb(IndexedWord w){
        return w.tag().equals(POS_TAG.VB) || w.tag().equals(POS_TAG.VBD) || w.tag().equals(POS_TAG.VBG) ||
                w.tag().equals(POS_TAG.VBN) || w.tag().equals(POS_TAG.VBP) || w.tag().equals(POS_TAG.VBZ);
    }

    /**
     * Checks if a word is some kind of a verb (i.e. if it has POS tag: VB, VBD, VBG, VBN, VBP or VBZ)
     * @param w: core label object
     * @return true if it is a verb, false otherwise
     */
    public static boolean isVerb(CoreLabel w){
        return w.tag().equals(POS_TAG.VB) || w.tag().equals(POS_TAG.VBD) || w.tag().equals(POS_TAG.VBG) ||
                w.tag().equals(POS_TAG.VBN) || w.tag().equals(POS_TAG.VBP) || w.tag().equals(POS_TAG.VBZ);
    }

    /**
     * Checks if a word is some kind of a noun (i.e. if it has POS tag: NN, NNS, NNP or NNPS)
     * @param w: indexed word
     * @return true if it is a noun, false otherwise
     */
    public static boolean isNoun(IndexedWord w){
        return w.tag().equals(POS_TAG.NN) ||w.tag().equals(POS_TAG.NNS) || w.tag().equals(POS_TAG.NNP) ||
                w.tag().equals(POS_TAG.NNPS);
    }

    /**
     * Checks if a word is some kind of an adjective (i.e. if it has POS tag: JJ, JJR or JJS)
     * @param w: indexed word
     * @return true if it is an adjective, false otherwise
     */
    public static boolean isAdj(IndexedWord w){
        return w.tag().equals(POS_TAG.JJ) || w.tag().equals(POS_TAG.JJR) || w.tag().equals(POS_TAG.JJS);
    }

    /**
     * Checks if a word is some kind of an adjective (i.e. if it has POS tag: JJ, JJR or JJS)
     * @param w: core label
     * @return true if it is an adjective, false otherwise
     */
    public static boolean isAdj(CoreLabel w){
        return w.tag().equals(POS_TAG.JJ) || w.tag().equals(POS_TAG.JJR) || w.tag().equals(POS_TAG.JJS);
    }

    /**
     * Checks if a word is some kind of an adverb (i.e. if it has POS tag: RB, RBR or RBS)
     * @param w: indexed word
     * @return true if it is an adverb, false otherwise
     */
    public static boolean isAdverb(IndexedWord w){
        return w.tag().equals(POS_TAG.RB) || w.tag().equals(POS_TAG.RBR) || w.tag().equals(POS_TAG.RBS);
    }

    /**
     * Checks if a word is some kind of an adverb (i.e. if it has POS tag: RB, RBR or RBS)
     * @param w: core label
     * @return true if it is an adverb, false otherwise
     */
    public static boolean isAdverb(CoreLabel w){
        return w.tag().equals(POS_TAG.RB) || w.tag().equals(POS_TAG.RBR) || w.tag().equals(POS_TAG.RBS);
    }

    /**
     * Checks if a word is some kind of a pronoun (i.e. if it has POS tag: PRP or PRP$)
     * @param w: Indexed word
     * @return true if it is a pronoun, false otherwise
     */
    public static boolean isPronoun(IndexedWord w){
        return w.tag().equals(POS_TAG.PRP) || w.tag().equals(POS_TAG.PRP_P);
    }

    /**
     * Checks if a word is some kind of a wh-pronoun (i.e. if it has POS tag: WP or WP$)
     * @param w: indexed word
     * @return true if it is a wh-pronoun, false otherwise
     */
    public static boolean isWhPronoun(IndexedWord w){
        return w.tag().equals(POS_TAG.WP) || w.tag().equals(POS_TAG.WP_P);
    }

    /**
     * @param pos POS tag
     * @return true -> if it is a verb; false -> otherwise
     */
    public static boolean isPOSVerb(String pos) {
        if (pos.length() < 2) {
            return false;
        }

        String test = pos.substring(0, 2);
        if (test.equals("VB")) {
            return true;
        }

        return false;

    }

    /**
     * @param pos POS tag
     * @return trye -> if it is a preposition; false -> otherwise
     */
    public static boolean isPOSPrep(String pos) {
        if (pos.equals(POS_TAG.IN) || pos.equals(POS_TAG.TO)) {
            return true;
        }
        return false;
    }
}
