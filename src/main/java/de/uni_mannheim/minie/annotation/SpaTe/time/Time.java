package de.uni_mannheim.minie.annotation.SpaTe.time;

import de.uni_mannheim.constant.NE_TYPE;
import de.uni_mannheim.constant.SEPARATOR;
import de.uni_mannheim.minie.annotation.SpaTe.SpaTeCore;
import edu.stanford.nlp.ling.IndexedWord;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.w3c.dom.*;

/**
 * @author Kiril Gashteovski
 */
public class Time implements Comparable<Time> {
    /** 4 possible types from TIMEX3 (UNKNOWN is just for debugging reasons, it must be one of the other 4) **/
    public enum Timex3Type {DATE, TIME, DURATION, SET, UNKNOWN}
    private static String EMPTY_STRING = "";

    /** ID of the temporal expression **/
    private String id;
    /** TIMEX3 type **/
    private Timex3Type type;
    /** Disambiguated value of the tempWords (SUTime style) **/
    private String value;
    /** TIMEX3 mod**/
    private String timex3mod;
    /** All temporal words (core words, premodifiers, postmodifiers, predicate) **/
    private SpaTeCore tempWords;
    /** The time expression in TIMEX3 XML tag **/
    private Element timex3Xml;

    public Time() {
        this.id = EMPTY_STRING;
        this.type = Timex3Type.UNKNOWN;
        this.value = EMPTY_STRING;
        this.timex3mod = EMPTY_STRING;
        this.tempWords = new SpaTeCore();
        this.timex3Xml = null;
    }

    public Time(Time t) {
        this.tempWords = t.getTemporalWords();
        this.id = t.getId();
        this.type = t.getType();
        this.value = t.getValue();
        this.timex3mod = EMPTY_STRING;
        this.timex3Xml = t.getTimex3Xml();
    }

    public Time(ObjectArrayList<IndexedWord> coreTimexWords) {
        this.id = EMPTY_STRING;
        this.type = Timex3Type.UNKNOWN;
        this.value = EMPTY_STRING;
        this.timex3mod = EMPTY_STRING;
        this.timex3Xml = null;
        this.tempWords = new SpaTeCore(coreTimexWords);
    }

    // Setters
    public void setTempWords(SpaTeCore tempWords) {
        this.tempWords = tempWords;
    }
    public void setId(String id) {
        this.id = id;
    }
    public void setType(Timex3Type type) {
        this.type = type;
    }
    public void setValue(String value) {
        this.value = value;
    }
    public void setTimex3mod(String mod) {
        this.timex3mod = mod;
    }
    public void setTimex3Xml(Element timex3Xml) {
        this.timex3Xml = timex3Xml;
    }
    public void setAllTemporalWords(ObjectArrayList<IndexedWord> allTempWords) {
        this.tempWords.setAllWords(allTempWords);
    }
    public void setPredicate(IndexedWord pred) {
        this.tempWords.setPredicate(pred);
    }

    // Getters
    public ObjectArrayList<IndexedWord> getCoreTemporalWords() {
        return this.tempWords.getCoreWords();
    }
    public String getId() {
        return this.id;
    }
    public Timex3Type getType() {
        return this.type;
    }
    public String getValue() {
        return this.value;
    }

    public ObjectArrayList<IndexedWord> getPreMods() {
        return this.tempWords.getPreMods();
    }
    public ObjectArrayList<IndexedWord> getPostMods() {
        return this.tempWords.getPostMods();
    }
    public ObjectArrayList<IndexedWord> getAllTemporalWords() {
        return this.tempWords.getAllWords();
    }
    public Element getTimex3Xml() {
        return this.timex3Xml;
    }
    public IndexedWord getPredicate() {
        return this.tempWords.getPredicate();
    }
    public SpaTeCore getTemporalWords() {
        return this.tempWords;
    }

    /**
     * @return the list of temporal words as a string
     */
    public String temporalWordsToString() {
        StringBuilder sb = new StringBuilder();
        for (IndexedWord w: this.tempWords.getCoreWords()) {
            sb.append(w.word());
            sb.append(SEPARATOR.SPACE);
        }
        sb.append(";");

        sb.append(" res:");
        sb.append(this.value);
        sb.append(SEPARATOR.SPACE);
        sb.append(";");

        if (!this.tempWords.getPreMods().isEmpty()) {
            sb.append("premods:");
            for (IndexedWord w: this.tempWords.getPreMods()) {
                sb.append(w.word());
                sb.append(SEPARATOR.SPACE);
            }
            sb.append(";");
        }

        if (!this.tempWords.getPostMods().isEmpty()) {
            sb.append("postmods:");
            for (IndexedWord w: this.tempWords.getPostMods()) {
                sb.append(w.word());
                sb.append(SEPARATOR.SPACE);
            }
            sb.append(";");
        }
        return sb.toString().trim();
    }

    public void addPreModWord(IndexedWord w) {
        this.tempWords.addWordToPreMods(w);
    }
    public void addPostModWord(IndexedWord w) {
        this.tempWords.addWordToPostMods(w);
    }

    /**
     * Given an indexed word w, remove it from all possible temporal word lists
     * (tempWords, preMods, postMods and allTemporalWords)
     * @param w word to be removed
     */
    public void removeTemporalWord(IndexedWord w) {
        this.tempWords.removeWord(w);
    }

    /**
     * Checks if the current time object is the same as t, by checking if the TimEx words are equal.
     * If so, return "true", otherwise return "fasle".
     * @param t time object
     * @return true/false if t's TimEx words are equal to the current object's TimEx words
     */
    public boolean isEqual(Time t) {
        return this.getTemporalWords().equals(t.getTemporalWords());
    }

    /**
     * If an IndexedWord is from NE Type different than a temporal one, then this word should be
     * ignored as a temporal expression.
     * @param word word to be inspected
     * @return true, if it should be ignored as a TempEx, false otherwise
     */
    public static boolean ignoreTempWord(IndexedWord word) {
        return word.ner().equals(NE_TYPE.PERSON) || word.ner().equals(NE_TYPE.LOCATION) ||
                word.ner().equals(NE_TYPE.ORGANIZATION) || word.ner().equals(NE_TYPE.MISC) ||
                word.ner().equals(NE_TYPE.MONEY);
    }

    /**
     * @return Flag indicating whether the TempEx has temporal predicate (relation)
     */
    public boolean hasPredicate() {
        return this.tempWords.getPredicate().index() != -1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (!this.getTemporalWords().getCoreWords().isEmpty()) {
            if (this.hasPredicate()) {
                sb.append("pred=");
                sb.append(this.tempWords.getPredicate().word());
                sb.append(SEPARATOR.COMMA);
                sb.append(SEPARATOR.SPACE);
            }
            sb.append(this.temporalWordsToString());
        } else {
            return "";
        }

        return sb.toString();
    }

    @Override
    public int compareTo(Time o) {
        return Integer.compare(this.tempWords.getCoreWords().get(0).index(), o.getCoreTemporalWords().get(0).index());
    }
}
