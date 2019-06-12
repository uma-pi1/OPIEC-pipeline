package de.uni_mannheim.minie.annotation.SpaTe;

import edu.stanford.nlp.ling.IndexedWord;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

/**
 * Class representing the core data structure for both space and time annotations information.
 *
 * @author Kiril Gashteovski
 */
public class SpaTeCore {
    /** Specific type (is it SPACE or TIME) **/
    public enum type {SPACE, TIME}
    /** The core words of space/time stuff **/
    private ObjectArrayList<IndexedWord> coreWords;
    /** The pre-modifiers of coreWords **/
    private ObjectArrayList<IndexedWord> preMods;
    /** The post-modifiers of coreWords **/
    private ObjectArrayList<IndexedWord> postMods;
    /** All the words (core, pre-mods and post-mods **/
    private ObjectArrayList<IndexedWord> allWords;
    /** Spatial/Temporal predicate **/
    private IndexedWord predicate;

    public SpaTeCore() {
        this.coreWords = new ObjectArrayList<>();
        this.preMods = new ObjectArrayList<>();
        this.postMods = new ObjectArrayList<>();
        this.allWords = new ObjectArrayList<>();
        this.predicate = new IndexedWord();
    }

    /**
     * @param coreWords only the core words and allwords are initialized (everything else is assumed to be empty)
     */
    public SpaTeCore(ObjectArrayList<IndexedWord> coreWords) {
        this.coreWords = coreWords.clone();
        this.allWords = coreWords.clone();
        this.preMods = new ObjectArrayList<>();
        this.postMods = new ObjectArrayList<>();
        this.predicate = new IndexedWord();
    }

    /** Setters **/
    public void setCoreWords(ObjectArrayList<IndexedWord> coreWords) {
        this.coreWords = coreWords;
    }

    public void setPreMods(ObjectArrayList<IndexedWord> preMods) {
        this.preMods = preMods;
    }

    public void setPostMods(ObjectArrayList<IndexedWord> postMods) {
        this.postMods = postMods;
    }

    public void setAllWords(ObjectArrayList<IndexedWord> allWords) {
        this.allWords = allWords;
    }
    public void setPredicate(IndexedWord predicate) {
        this.predicate = predicate;
    }

    /** Getters **/
    public ObjectArrayList<IndexedWord> getCoreWords() {
        return this.coreWords;
    }
    public ObjectArrayList<IndexedWord> getPreMods() {
        return this.preMods;
    }
    public ObjectArrayList<IndexedWord> getPostMods() {
        return this.postMods;
    }
    public ObjectArrayList<IndexedWord> getAllWords() {
        return this.allWords;
    }
    public IndexedWord getPredicate() {
        return this.predicate;
    }

    /** Adding words to lists **/
    public void addWordToCoreWords(IndexedWord w) {
        this.coreWords.add(w);
    }
    public void addWordToPreMods(IndexedWord w) {
        this.preMods.add(w);
    }
    public void addWordToPostMods(IndexedWord w) {
        this.postMods.add(w);
    }
    public void addWordToAllWords(IndexedWord w) {
        this.allWords.add(w);
    }

    /** Remove a word from the whole structure **/
    public void removeWord(IndexedWord w) {
        this.coreWords.remove(w);
        this.postMods.remove(w);
        this.preMods.remove(w);
        this.allWords.remove(w);
        if (this.predicate.equals(w)) {
            this.predicate = new IndexedWord();
        }
    }

    /**
     * @return Flag indicating whether the TempEx has temporal predicate (relation)
     */
    public boolean hasPredicate() {
        return this.predicate.index() != -1;
    }

    /** Cloning method **/
    public SpaTeCore clone() {
        SpaTeCore core = new SpaTeCore();
        core.setAllWords(this.allWords.clone());
        core.setCoreWords(this.coreWords.clone());
        core.setPreMods(this.preMods.clone());
        core.setPostMods(this.postMods.clone());
        core.setPredicate(this.predicate);
        return core;
    }
}
