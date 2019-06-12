package de.uni_mannheim.minie.annotation.SpaTe.space;

import de.uni_mannheim.constant.SEPARATOR;
import de.uni_mannheim.minie.annotation.SpaTe.SpaTeCore;
import edu.stanford.nlp.ling.IndexedWord;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

/**
 * @author Kiril Gashteovski
 */
public class Space implements Comparable<Space> {
    // Attributes
    private SpaTeCore spatialWords;

    // Constructors
    public Space() {
        this.spatialWords = new SpaTeCore();
    }
    public Space(ObjectArrayList<IndexedWord> coreTimexWords) {
        this.spatialWords = new SpaTeCore(coreTimexWords);
    }
    public Space(Space sp) {
        this.spatialWords = sp.getSpatialWords();
    }

    // Getters
    public SpaTeCore getSpatialWords() {
        return this.spatialWords;
    }
    public ObjectArrayList<IndexedWord> getCoreSpatialWords() {
        return this.spatialWords.getCoreWords();
    }
    public ObjectArrayList<IndexedWord> getAllSpatialWords() {
        return this.spatialWords.getAllWords();
    }
    public ObjectArrayList<IndexedWord> getPreMods() {
        return this.spatialWords.getPreMods();
    }
    public ObjectArrayList<IndexedWord> getPostMods() {
        return this.spatialWords.getPostMods();
    }
    public IndexedWord getPredicate() {
        return this.spatialWords.getPredicate();
    }

    // Setters
    public void setPredicate(IndexedWord predicate) {
        this.spatialWords.setPredicate(predicate);
    }
    public void setCoreSpatialWords(ObjectArrayList<IndexedWord> spatialWords) {
        this.spatialWords.setCoreWords(spatialWords);
    }
    public void setAllSpatialWords(ObjectArrayList<IndexedWord> spatialWords) {
        this.spatialWords.setAllWords(spatialWords);
    }

    // Adders
    public void addSpatialWord(IndexedWord w) {
        this.spatialWords.addWordToAllWords(w);
        this.spatialWords.addWordToCoreWords(w);
    }
    public void addPreModWord(IndexedWord w) {
        this.spatialWords.addWordToPreMods(w);
    }
    public void addPostModWord(IndexedWord w) {
        this.spatialWords.addWordToPostMods(w);
    }

    /**
     * Given an indexed word w, remove it from all possible spatial word lists
     * (coreWords, preMods, postMods and allTemporalWords)
     *
     * @param w word to be removed
     */
    public void removeSpatialWord(IndexedWord w) {
        this.spatialWords.removeWord(w);
    }

    /**
     * Checks if the current space object is the same as s, by checking if the SpacEx words are equal.
     * If so, return "true", otherwise return "fasle".
     * @param s space object
     * @return true/false if t's SpacEx words are equal to the current object's SpacEx words
     */
    public boolean isEqual(Space s) {
        return this.getSpatialWords().equals(s.getSpatialWords());
    }

    /**
     * @return Flag indicating whether the TempEx has temporal predicate (relation)
     */
    public boolean hasPredicate() {
        return this.spatialWords.getPredicate().index() != -1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (!this.getSpatialWords().getCoreWords().isEmpty()) {
            if (this.hasPredicate()) {
                sb.append("pred=");
                sb.append(this.spatialWords.getPredicate().word());
                sb.append(SEPARATOR.COMMA);
                sb.append(SEPARATOR.SPACE);
            }
            sb.append(this.spatialWordsToString());
        } else {
            return "";
        }

        return sb.toString();
    }

    /**
     * @return the list of temporal words as a string
     */
    public String spatialWordsToString() {
        StringBuilder sb = new StringBuilder();
        for (IndexedWord w: this.spatialWords.getCoreWords()) {
            sb.append(w.word());
            sb.append(SEPARATOR.SPACE);
        }
        sb.append(";");

        if (!this.spatialWords.getPreMods().isEmpty()) {
            sb.append("premods:");
            for (IndexedWord w: this.spatialWords.getPreMods()) {
                sb.append(w.word());
                sb.append(SEPARATOR.SPACE);
            }
            sb.append(";");
        }

        if (!this.spatialWords.getPostMods().isEmpty()) {
            sb.append("postmods:");
            for (IndexedWord w: this.spatialWords.getPostMods()) {
                sb.append(w.word());
                sb.append(SEPARATOR.SPACE);
            }
            sb.append(";");
        }
        return sb.toString().trim();
    }

    @Override
    public int compareTo(Space o) {
        return Integer.compare(this.spatialWords.getCoreWords().get(0).index(), o.getCoreSpatialWords().get(0).index());
    }
}
