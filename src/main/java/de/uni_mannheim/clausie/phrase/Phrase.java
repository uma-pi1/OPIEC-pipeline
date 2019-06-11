package de.uni_mannheim.clausie.phrase;

import java.util.List;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.trees.TypedDependency;
import edu.stanford.nlp.util.CoreMap;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import de.uni_mannheim.constant.SEPARATOR;
import de.uni_mannheim.utils.coreNLP.CoreNLPUtils;

/**
 *
 * @author Kiril Gashteovski
 */
public class Phrase {
    /** A list of words of the phrase **/
    protected ObjectArrayList<IndexedWord> wordList;
    /** The dependency parse graph of the phrase **/
    protected SemanticGraph phraseGraph;
    /** The head word of the phrase **/
    protected IndexedWord head;
    /** List of typed dependencies for the phrase **/
    protected ObjectArrayList<TypedDependency> tds;

    //XXX
    private boolean processedConjunction;
    public boolean isProcessedConjunction() {
        return processedConjunction;
    }
    public void setProcessedConjunction(boolean processedConjunction) {
        this.processedConjunction = processedConjunction;
    }
    private IndexedWord conjWord;
    public IndexedWord getConjWord() {
        return conjWord;
    }
    public void setConjWord(IndexedWord conjWord) {
        this.conjWord = conjWord;
    }

    /** Constructors **/
    public Phrase(){
        this.wordList = new ObjectArrayList<>();
        this.phraseGraph = new SemanticGraph();
        this.head = new IndexedWord();
        this.tds = new ObjectArrayList<>();
    }

    /** Copy constructor **/
    public Phrase(Phrase p){
        //XXX change parameter
        this(p.getWordList().clone(), new SemanticGraph(p.getPhraseGraph()), new IndexedWord(p.getHeadWord()),
                p.getTypedDependencies().clone(), p.isProcessedConjunction(), p.getConjWord());
    }

    /**
     * Parametric constructor with a list of words for the phrase. The head word, phrase graph and the list of typed dependencies
     * are empty.
     * @param wList: list of words for the phrase
     */
    public Phrase(ObjectArrayList<IndexedWord> wList) {
        this.wordList = wList;
        this.phraseGraph = new SemanticGraph();
        this.head = new IndexedWord();
        this.tds = new ObjectArrayList<>();
    }

    /**
     * Parametric constructor with a list of words for the phrase and the semantic graph of the phrase. The head word,
     * and the list of typed dependencies are empty.
     * @param wList: list of words for the phrase
     * @param sg: semantic graph for the phrase
     */
    public Phrase(ObjectArrayList<IndexedWord> wList, SemanticGraph sg){
        this.wordList = wList;
        this.phraseGraph = sg;
        this.head = new IndexedWord();
        this.tds = new ObjectArrayList<>();
    }

    /**
     * Parametric constructor with a list of words for the phrase, the semantic graph of the phrase and the head word of the
     * phrase. The list of typed dependencies is empty.
     * @param wList: list of words for the phrase
     * @param sg: semantic graph of the phrase
     * @param h: head word of the phrase
     */
    public Phrase(ObjectArrayList<IndexedWord> wList, SemanticGraph sg, IndexedWord h){
        this.wordList = wList;
        this.phraseGraph = sg;
        this.head = h;
        this.tds = new ObjectArrayList<>();
    }

    /**
     * Parametric constructor with a list of words for the phrase, and the head word of the phrase.
     * The list of typed dependencies and the semantic graph of the phrase are empty.
     * @param wList: list of words for the phrase
     * @param h: the head word of the phrase
     */
    public Phrase(ObjectArrayList<IndexedWord> wList, IndexedWord h) {
        this.wordList = wList;
        this.head = h;
        this.phraseGraph = new SemanticGraph();
        this.tds = new ObjectArrayList<>();
    }

    /**
     * Parametric constructor with a list of words for the phrase, the semantic graph of the phrase, the head word of the
     * and the list of typed dependencies.
     * @param wList: list of words for the phrase
     * @param sg: the semantic graph of the phrase
     * @param r: the head word of the phrase
     * @param td: list of typed dependencies for the phrase
     */
    public Phrase(ObjectArrayList<IndexedWord> wList, SemanticGraph sg, IndexedWord r, ObjectArrayList<TypedDependency> td, boolean processedConjunction, IndexedWord conjWord){
        this.wordList = wList;
        this.phraseGraph = sg;
        this.head = r;
        this.tds = td;
        this.processedConjunction = processedConjunction;
        this.conjWord = conjWord;
    }

    /** Add indexed word to the word list **/
    public void addWordToList(IndexedWord word){
        this.wordList.add(word);
    }
    /** Add a list of indexed words to the word list **/
    public void addWordsToList(ObjectArrayList<IndexedWord> words){
        this.wordList.addAll(words);
    }

    /** Remove a set of words from the list (given a set of words) **/
    public void removeWordsFromList(ObjectOpenHashSet<IndexedWord> words){
        this.wordList.removeAll(words);
    }
    /** Remove a set of words represented as core labels from the list of indexed words **/
    public void removeCoreLabelWordsFromList(List<CoreMap> cmWords){
        ObjectArrayList<IndexedWord> rWords = new ObjectArrayList<>();
        for (CoreMap cm: cmWords){
            rWords.add(new IndexedWord(new CoreLabel(cm)));
        }
        this.removeWordsFromList(rWords);
    }

    public void removeWordsFromList(ObjectArrayList<IndexedWord> words){
        this.wordList.removeAll(words);
    }

    /** Getters **/
    public ObjectArrayList<IndexedWord> getWordList(){
        return this.wordList;
    }
    public ObjectArrayList<IndexedWord> getWordSubList(int from, int to){
        ObjectArrayList<IndexedWord> sublist = new ObjectArrayList<>();
        for (int i = from; i <= to; i++){
            sublist.add(this.wordList.get(i));
        }
        return sublist;
    }
    public IndexedWord getHeadWord(){
        return this.head;
    }
    public SemanticGraph getPhraseGraph(){
        return this.phraseGraph;
    }
    public ObjectArrayList<TypedDependency> getTypedDependencies(){
        return this.tds;
    }
    public ObjectArrayList<CoreLabel> getWordCoreLabelList(){
        ObjectArrayList<CoreLabel> clList = new ObjectArrayList<>();
        for (IndexedWord w: this.wordList){
            clList.add(new CoreLabel(w));
        }
        return clList;
    }

    public void setHeadWord(IndexedWord h){
        this.head = h;
    }

    /**
     * Replace the word in the list on the i-th position with the new word 
     * @param i: the position of the word in the wordlist to be replaced
     * @param newWord: the new word to be put in the i-th position 
     */
    public void setWordInWordList(int i, IndexedWord newWord){
        this.wordList.set(i, newWord);
    }

    /**
     * Given a list of words 'words', replace all of them with one word 'w'
     * @param words: the list of words to be replaced with one word
     * @param w: the word replacing the sublist
     */
    public void replaceWordsWithOneWord(ObjectArrayList<IndexedWord> words, IndexedWord w){
        // If the list of words is empty, return (nothing to replace)
        if (words.size() == 0)
            return;

        // Replace the first word from the list with the replacing word, then drop it from the list, then remove the rest
        int firstWordInd = this.getWordList().indexOf(words.get(0));
        if (firstWordInd == -1)
            return;
        this.setWordInWordList(firstWordInd, w);
        words.remove(0);
        if (words.size() > 0)
            this.removeWordsFromList(words);
    }

    /**
     * Return a string of words, containing the phrase, in the following format: word1 word2 ... wordn
     * @return a string in the format: word1 word2 ... wordn
     */
    public String getWords(){
        StringBuilder sbWords = new StringBuilder();

        for (IndexedWord word: this.wordList){
            sbWords.append(word.word());
            sbWords.append(SEPARATOR.SPACE);
        }

        return sbWords.toString().trim();
    }

    /**
     * Given the sentence semantic graph, detect the head word of the phrase (i.e. the word with shortest path to the
     * root word of the sentence)
     *
     * @param sentenceSg: semantic graph of the sentence
     */
    public void detectHeadWord(SemanticGraph sentenceSg){
        this.head = CoreNLPUtils.getHeadWord(this.wordList, sentenceSg);
    }

    /**
     * @param w word that could be in the phrase
     * @return true, if the phrase contains w, false otherwise
     */
    public boolean contains(IndexedWord w) {
        return this.wordList.contains(w);
    }

    public void clear() {
        this.phraseGraph = new SemanticGraph();
        this.head = new IndexedWord();
        this.tds.clear();
        this.wordList.clear();
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (IndexedWord w: this.getWordList()) {
            sb.append(w.word());
            sb.append(SEPARATOR.SPACE);
        }
        return sb.toString().trim();
    }
}
