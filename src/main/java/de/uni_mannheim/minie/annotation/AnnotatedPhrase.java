package de.uni_mannheim.minie.annotation;

import java.util.*;

import de.uni_mannheim.clausie.phrase.Phrase;
import de.uni_mannheim.constant.CHARACTER;
import de.uni_mannheim.constant.NE_TYPE;
import de.uni_mannheim.constant.POS_TAG;
import de.uni_mannheim.constant.REGEX;
import de.uni_mannheim.constant.SEPARATOR;

import de.uni_mannheim.minie.annotation.SpaTe.SpaTeCore;
import de.uni_mannheim.minie.annotation.SpaTe.space.Space;
import de.uni_mannheim.minie.annotation.SpaTe.time.Time;
import de.uni_mannheim.utils.coreNLP.WordCollectionUtils;
import de.uni_mannheim.utils.coreNLP.WordUtils;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.ling.tokensregex.TokenSequenceMatcher;
import edu.stanford.nlp.ling.tokensregex.TokenSequencePattern;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphEdge;
import edu.stanford.nlp.semgraph.SemanticGraphUtils;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.util.CoreMap;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import de.uni_mannheim.utils.coreNLP.CoreNLPUtils;

/**
 * The annotated phrase is a phrase that holds some sort of annotations. For now, the only annotation
 * that a phrase has are the quantities. Each phrase has a list of quantities.
 *
 * @author Kiril Gashteovski
 *
 */
public class AnnotatedPhrase extends Phrase {
    /** The list of quantities for the phrase **/
    private ObjectArrayList<Quantity> quantities;
    /** List of dropped words **/
    private ObjectOpenHashSet<IndexedWord> droppedWords;
    /** Space annotation **/
    private Space space;
    /** Word modified by the spatial annotation **/
    private IndexedWord spacemod;
    private boolean spatialflag;
    /** Time annotation **/
    private Time time;
    /** Word modified by the temporal annotation **/
    private IndexedWord tempmod;
    private boolean temporalflag;

    /** Default constructor **/
    public AnnotatedPhrase(){
        super();
        this.quantities = new ObjectArrayList<>();
        this.droppedWords = new ObjectOpenHashSet<>();
        this.time = new Time();
        this.tempmod = new IndexedWord();
        this.temporalflag = false;
        this.space = new Space();
        this.spacemod = new IndexedWord();
        this.spatialflag = false;
    }

    /**
     * Parametric constructor 
     * @param p: the phrase to be annotated
     * @param q: the quantities for phrase 'p'
     */
    public AnnotatedPhrase(Phrase p, ObjectArrayList<Quantity> q){
        super(p);
        this.quantities = q;
        this.droppedWords = new ObjectOpenHashSet<>();
        this.time = new Time();
        this.tempmod = new IndexedWord();
        this.temporalflag = false;
        this.space = new Space();
        this.spacemod = new IndexedWord();
        this.spatialflag = false;
    }

    /**
     * Parametric constructor: given a list of indexed words, create annotated phrase with empty quantities list
     * @param wList: list of indexed words for the phrase
     */
    public AnnotatedPhrase(ObjectArrayList<IndexedWord> wList) {
        super(wList);
        this.quantities = new ObjectArrayList<>();
        this.droppedWords = new ObjectOpenHashSet<>();
        this.time = new Time();
        this.tempmod = new IndexedWord();
        this.temporalflag = false;
        this.space = new Space();
        this.spacemod = new IndexedWord();
        this.spatialflag = false;
    }

    /**
     * Parametric constructor: given a list of indexed words and the root of the phrase, create annotated phrase with 
     * empty quantities list.
     * @param wList: list of words for the phrase
     * @param root: the root of the phrase
     */
    public AnnotatedPhrase(ObjectArrayList<IndexedWord> wList, IndexedWord root) {
        super(wList, root);
        this.quantities = new ObjectArrayList<>();
        this.droppedWords = new ObjectOpenHashSet<>();
        this.time = new Time();
        this.tempmod = new IndexedWord();
        this.temporalflag = false;
        this.space = new Space();
        this.spacemod = new IndexedWord();
        this.spatialflag = false;
    }

    /**
     * Parametric constructor: given a list of indexed words and semantic graph, create annotated phrase, with empty
     * quantities list
     * @param wList: the list of words for the phrase
     * @param sg: the semantic graph of the phrase (should be the sentence subgraph)
     */
    public AnnotatedPhrase(ObjectArrayList<IndexedWord> wList, SemanticGraph sg){
        super(wList, sg);
        this.quantities = new ObjectArrayList<>();
        this.droppedWords = new ObjectOpenHashSet<>();
        this.time = new Time();
        this.tempmod = new IndexedWord();
        this.temporalflag = false;
        this.space = new Space();
        this.spacemod = new IndexedWord();
        this.spatialflag = false;
    }

    /**
     * Parametric constructor: given a list of indexed words, semantic graph, and a root word, create annotated phrase, with 
     * empty quantities list
     * @param wList: the list of words for the phrase
     * @param sg: the semantic graph of the phrase (should be the sentence subgraph)
     * @param root: the root of the phrase
     */
    public AnnotatedPhrase(ObjectArrayList<IndexedWord> wList, SemanticGraph sg, IndexedWord root){
        super(wList, sg, root);
        this.quantities = new ObjectArrayList<>();
        this.droppedWords = new ObjectOpenHashSet<>();
        this.time = new Time();
        this.tempmod = new IndexedWord();
        this.temporalflag = false;
        this.space = new Space();
        this.spacemod = new IndexedWord();
        this.spatialflag = false;
    }

    /**
     * Parametric constructor: given a phrase as a parameter, set it as a 'phrase', and make an empty quantities list
     * @param p: the phrase to be initialized
     */
    public AnnotatedPhrase(Phrase p){
        super(p);
        this.quantities = new ObjectArrayList<>();
        this.droppedWords = new ObjectOpenHashSet<>();
        this.time = new Time();
        this.tempmod = new IndexedWord();
        this.temporalflag = false;
        this.space = new Space();
        this.spacemod = new IndexedWord();
        this.spatialflag = false;
    }

    /**
     * Copy constructor
     * @param ap: object to be copied
     */
    public AnnotatedPhrase(AnnotatedPhrase ap){
        super(ap.getWordList());
        this.quantities = ap.getQuantities();
        this.droppedWords = ap.getDroppedWords();
        this.time = ap.getTime();
        this.tempmod = ap.getTemporallyModifiedWord();
        this.temporalflag = ap.hasTempEx();
        this.space = ap.getSpace();
        this.spacemod = ap.getSpatiallyModifiedWord();
        this.spatialflag = ap.hasSpaceEx();
    }

    public boolean hasTempEx() {
        return this.temporalflag;
    }

    /** Get the quantities **/
    public ObjectArrayList<Quantity> getQuantities(){
        return this.quantities;
    }

    /** Set the quantities **/
    public void setQuantities(ObjectArrayList<Quantity> q){
        this.quantities = q;
    }

    /**
     * Detect the quantities in a phrase (given the sentence semantic graph).
     * @param sentSemGraph: the sentence semantic graph
     */
    public void detectQuantities(SemanticGraph sentSemGraph, int i){
        // Quantity words and edges
        ObjectArrayList<IndexedWord> qWords;

        // Tokens regex patterns
        String tokenRegexPattern;
        if (i == 1)
            tokenRegexPattern = REGEX.QUANTITY_SEQUENCE;
        else
            tokenRegexPattern = REGEX.QUANTITY_SEQUENCE_WITH_NO;

        TokenSequencePattern tPattern = TokenSequencePattern.compile(tokenRegexPattern);
        TokenSequenceMatcher tMatcher = tPattern.getMatcher(this.getWordCoreLabelList());

        // Some reusable variables
        List<CoreMap> matchCoreMaps;
        ObjectOpenHashSet<IndexedWord> wordsSet = new ObjectOpenHashSet<>();
        IndexedWord head;
        Set<SemanticGraphEdge> subtreeedges = new HashSet<>();
        int matchCounter = -1;

        // Annotate the matches and their subtrees
        while (tMatcher.find()){
            matchCounter++;
            matchCoreMaps = tMatcher.groupNodes();

            // Get the head word of the phrase and see whether or not to add it to the quantities
            head = CoreNLPUtils.getRootFromCoreMapWordList(sentSemGraph, matchCoreMaps);
            if (head.ner().equals(NE_TYPE.DATE) || head.ner().equals(NE_TYPE.LOCATION) ||
                    head.ner().equals(NE_TYPE.MISC) || head.ner().equals(NE_TYPE.ORGANIZATION) ||
                    head.ner().equals(NE_TYPE.PERSON) || head.ner().equals(NE_TYPE.TIME))
                continue;

            // Add the subtree elements of the head word if the right relations are in force
            for (IndexedWord w: sentSemGraph.getChildren(head)){
                if ((sentSemGraph.reln(head, w) == EnglishGrammaticalRelations.QUANTIFIER_MODIFIER) ||
                        (sentSemGraph.reln(head, w) == EnglishGrammaticalRelations.ADVERBIAL_MODIFIER)){
                    wordsSet.add(w);
                    subtreeedges = CoreNLPUtils.getSubTreeEdges(w, sentSemGraph, null);
                }
            }

            // Add the quantity words found and annotate them within the phrase
            wordsSet.addAll(WordCollectionUtils.getWordSet(matchCoreMaps));
            wordsSet.addAll(WordCollectionUtils.getSortedWordsFromListOfEdges(subtreeedges));
            wordsSet.retainAll(this.getWordList());
            qWords = WordCollectionUtils.getSortedWords(wordsSet);
            if (qWords.isEmpty()) {
                continue;
            }
            this.setQuantitiesFromWordList(qWords.clone(), i, matchCounter);

            // Reset
            qWords.clear();
            wordsSet.clear();
        }
    }

    /**
     * Given a quantity ID, return the quantity with that particular ID. If there is no quantity with that ID, 
     * return null.
     * @param qId: quantity ID
     */
    public Quantity getQuantityByID(String qId){
        for (Quantity quantity : this.quantities) {
            if (quantity.getId().equals(qId)) {
                return quantity;
            }
        }
        return null;
    }

    /**
     * Given a quantity ID, remove it from the list of quantities in the annotated phrase. If there is no quantity 
     * by that ID, nothing happens.
     * @param qId: quantity ID.
     */
    public void removeQuantityByID(String qId){
        int remInd = -1;
        for (int i = 0; i < this.quantities.size(); i++){
            if (this.quantities.get(i).getId().equals(qId)){
                remInd = i;
                break;
            }
        }
        if (remInd > -1){
            this.quantities.remove(remInd);
        }
    }

    /**
     * A helper function used in detectQuantities. When we have a list of quantity words,
     * add quantities to the list of quantities and clear the reusable lists.
     *  If there are quantities in the phrase, replace them with the word SOME_n_i, where i = the place of the quantity
     * (0 - subject, 1 - relation, 2 - object) and j = # of quantity within the phrase.
     *
     * @param qWords: list of quantity indexed words
     * @param i: used for ID-ying purposes of the quantities' annotations
     * @param j: used for ID-ying purposes of the quantities' annotations 
     */
    private void setQuantitiesFromWordList(ObjectArrayList<IndexedWord> qWords, int i, int j){
        // Quantity ID
        StringBuilder sbId = new StringBuilder();
        if (i == 0)
            sbId.append(Quantity.SUBJECT_ID);
        else if (i == 1)
            sbId.append(Quantity.RELATION_ID);
        else
            sbId.append(Quantity.OBJECT_ID);
        sbId.append(CHARACTER.UNDERSCORE);
        sbId.append(j + 1); // Indexing starts from 1

        // Add the quantity to the list
        this.quantities.add(new Quantity(qWords, sbId.toString()));

        // Clear the lists
        qWords.clear();
    }

    /**
     * If there are quantities in the phrase, replace them with the word QUANT_n_i, where i = the place of the quantity
     * (0 - subject, 1 - relation, 2 - object) and j = # of quantity within the phrase.
     *
     */
    public void annotateQuantities(){
        IndexedWord w;
        StringBuilder sbID = new StringBuilder();
        if (this.quantities.size() > 0){
            // Replacing word
            for (Quantity quantity : this.quantities) {
                w = new IndexedWord(quantity.getQuantityWords().get(0));

                sbID.append(Quantity.ST_QUANT);
                sbID.append(CHARACTER.UNDERSCORE);
                sbID.append(quantity.getId());

                w.setWord(sbID.toString());
                w.setLemma(sbID.toString());
                w.setTag(Quantity.ST_QUANTITY);
                w.setNER(Quantity.ST_QUANTITY);

                this.replaceWordsWithOneWord(quantity.getQuantityWords().clone(), w);
                sbID.setLength(0);
            }
        }

        // Merge the adjacent quantities
        this.mergeAdjacentQuantities();
    }

    /**
     * When there are already annotated quantities, merge the ones which are right next to each other in a sequence.
     */
    public void mergeAdjacentQuantities(){
        // Reusable variables
        ObjectArrayList<IndexedWord> mergedQuantityWords = new ObjectArrayList<>();
        ObjectArrayList<String> qIds = new ObjectArrayList<>();
        ObjectOpenHashSet<IndexedWord> remWords = new ObjectOpenHashSet<>();
        ObjectArrayList<IndexedWord> matches;

        // Token regex pattern and matcher
        TokenSequencePattern tPattern = TokenSequencePattern.compile(REGEX.ADJACENT_QUANTITIES);
        TokenSequenceMatcher tMatcher = tPattern.getMatcher(this.getWordCoreLabelList());

        // Merge the quantities when matched
        while (tMatcher.find()){
            // Get the merged words and edges from the quantities that should be merged.
            matches = WordCollectionUtils.getWordList(tMatcher.groupNodes());

            for (IndexedWord matche : matches) {
                // If it has preposition bridging two quantities, add it to the mergedQuantityWords list
                if (matche.tag().equals(POS_TAG.IN)) {
                    mergedQuantityWords.add(matches.get(1));
                    remWords.add(matches.get(1));
                }

                // Merge the adjacent quantities
                for (Quantity q : this.getQuantities()) {
                    if ((Quantity.ST_QUANT + CHARACTER.UNDERSCORE + q.getId()).equals(matche.word())) {
                        qIds.add(q.getId());
                        mergedQuantityWords.addAll(q.getQuantityWords());
                    }
                }
            }

            // Add all the words and edges from the merged quantities to the first one and remove the rest
            for (int i = 0; i < this.getWordList().size(); i++){
                if (this.getWordList().get(i).word().equals(Quantity.ST_QUANT + CHARACTER.UNDERSCORE + qIds.get(0))){
                    if (this.getQuantityByID(qIds.get(0)) != null){
                        this.getQuantityByID(qIds.get(0)).setWords(mergedQuantityWords);
                        for (int j = 1; j < qIds.size(); j++){
                            this.removeQuantityByID(qIds.get(j));
                            for (int k = i; k < this.getWordList().size(); k++){
                                if (this.getWordList().get(k).word().equals(Quantity.ST_QUANT + CHARACTER.UNDERSCORE +
                                        qIds.get(j))){
                                    remWords.add(this.getWordList().get(k));
                                }
                            }
                        }
                        break;
                    }
                }
            }

            // Remove and clear 
            this.removeWordsFromList(remWords);
            remWords.clear();
            qIds.clear();
        }
    }

    public Time getTime() {
        return this.time;
    }

    public Space getSpace() {
        return this.space;
    }

    public boolean hasSpaceEx() {
        return this.spatialflag;
    }

    public IndexedWord getSpatiallyModifiedWord() {
        return this.spacemod;
    }

    public IndexedWord getTemporallyModifiedWord() {
        return this.tempmod;
    }

    public void setTime(Time t) {
        this.time = t;
    }

    public void setSpace(Space s) {
        this.space = s;
    }

    @Override
    public void clear() {
        this.phraseGraph = new SemanticGraph();
        this.head = new IndexedWord();
        this.tds.clear();
        this.wordList.clear();
        this.quantities.clear();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (IndexedWord w: this.wordList) {
            sb.append(w.word());
            sb.append(SEPARATOR.SPACE);
        }
        return sb.toString().trim();
    }

    /**
     * Given a list of indexed words (the ones that need to be dropped), add all of them to the 
     * set of dropped words.
     * @param droppedWords: list of words (IndexedWord objects) that need to be dropped from the phrase
     */
    public void addDroppedWords(ObjectArrayList<IndexedWord> droppedWords) {
        this.droppedWords.addAll(droppedWords);
    }
    public void addDroppedWords(ObjectOpenHashSet<IndexedWord> droppedWords) {
        this.droppedWords.addAll(droppedWords);
    }

    public ObjectOpenHashSet<IndexedWord> getDroppedWords() {
        return this.droppedWords;
    }

    /**
     * Detect time in an annotated phrase (e.g. "17th-century park" -> "17th-century" is TempEx
     */
    public void detectTime(ObjectArrayList<Time> tempAnnotations, SemanticGraph sg) {
        // Get the indices of the words
        IntArrayList temporalIndices = new IntArrayList();
        if (!tempAnnotations.isEmpty()) {
            for (Time ta: tempAnnotations) {
                for (IndexedWord w: ta.getCoreTemporalWords()) {
                    temporalIndices.add(w.index());
                }
            }
        }

        if (!tempAnnotations.isEmpty()) {
            for (IndexedWord w : this.wordList) {
                if (w.index() < 0) {
                    continue;
                }
                if (WordUtils.isNoun(w) || WordUtils.isAdj(w)) {
                    this.detectSpaceTimeWithReln(sg, w, EnglishGrammaticalRelations.ADJECTIVAL_MODIFIER, temporalIndices, SpaTeCore.type.TIME);
                    this.detectSpaceTimeWithReln(sg, w, EnglishGrammaticalRelations.ADJECTIVAL_COMPLEMENT, temporalIndices, SpaTeCore.type.TIME);
                    this.detectSpaceTimeWithReln(sg, w, EnglishGrammaticalRelations.ADVERBIAL_MODIFIER, temporalIndices, SpaTeCore.type.TIME);
                    this.detectSpaceTimeWithReln(sg, w, EnglishGrammaticalRelations.NOUN_COMPOUND_MODIFIER, temporalIndices, SpaTeCore.type.TIME);
                    this.detectSpaceTimeWithReln(sg, w, EnglishGrammaticalRelations.NUMBER_MODIFIER, temporalIndices, SpaTeCore.type.TIME);
                    this.detectSpaceTimeWithReln(sg, w, EnglishGrammaticalRelations.NUMERIC_MODIFIER, temporalIndices, SpaTeCore.type.TIME);
                    this.detectSpaceTimeWithReln(sg, w, EnglishGrammaticalRelations.TEMPORAL_MODIFIER, temporalIndices, SpaTeCore.type.TIME);
                    // TODO: use NP_ADVERBIAL_MODIFIER, OBJECT and INDIRECT_OBJECT as a dependency for improving the quantities
                    //this.detectTimeWithReln(sg, w, EnglishGrammaticalRelations.OBJECT, temporalIndices);
                }
            }
        }
    }

    public void detectSpace(ObjectArrayList<Space> spaceAnnotations, SemanticGraph sg) {
        // Get the indices of the words
        IntArrayList spatialIndices = new IntArrayList();
        if (!spaceAnnotations.isEmpty()) {
            for (Space s: spaceAnnotations) {
                for (IndexedWord w: s.getCoreSpatialWords()) {
                    spatialIndices.add(w.index());
                }
            }
        }

        if (!spaceAnnotations.isEmpty()) {
            for (IndexedWord w : this.wordList) {
                if (w.index() < 0) {
                    continue;
                }
                if (WordUtils.isNoun(w) || WordUtils.isAdj(w)) {
                    this.detectSpaceTimeWithReln(sg, w, EnglishGrammaticalRelations.ADJECTIVAL_MODIFIER, spatialIndices, SpaTeCore.type.SPACE);
                    this.detectSpaceTimeWithReln(sg, w, EnglishGrammaticalRelations.ADJECTIVAL_COMPLEMENT, spatialIndices, SpaTeCore.type.SPACE);
                    this.detectSpaceTimeWithReln(sg, w, EnglishGrammaticalRelations.ADVERBIAL_MODIFIER, spatialIndices, SpaTeCore.type.SPACE);
                    this.detectSpaceTimeWithReln(sg, w, EnglishGrammaticalRelations.NOUN_COMPOUND_MODIFIER, spatialIndices, SpaTeCore.type.SPACE);
                }
            }
        }
    }

    /**
     * Detect time/space given a head word, a concrete grammatical relation and the temporal/spatial indices
     *
     * @param sg semantic graph of the sentence
     * @param head head word being examined
     * @param gr grammatical relation (dependency type)
     * @param spaceTimeIndices temporal/spatial indices of the TempEx/SpaceEx
     * @param t either TIME or SPACE
     */
    private void detectSpaceTimeWithReln(SemanticGraph sg, IndexedWord head, GrammaticalRelation gr,
                                         IntArrayList spaceTimeIndices, SpaTeCore.type t) {
        for (IndexedWord child: sg.getChildrenWithReln(head, gr)) {
            // Special case for 'prep' relation (e.g. "Paris in Texas", "Ferdinand of Austria", etc.)
            Set<IndexedWord> prepChildren = new HashSet<>();
            IndexedWord prepLocGrandChild = new IndexedWord();
            if (gr == EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER) {
                prepChildren = sg.getChildren(child);
                for (IndexedWord w : prepChildren) {
                    if (spaceTimeIndices.contains(w.index())) {
                        prepLocGrandChild = w;
                    }
                }
            }

            boolean spaceTimeIndicesCheck = spaceTimeIndices.contains(child.index()) && !spaceTimeIndices.contains(head.index());
            boolean spaceTimeIndicesCheckPrep = (gr == EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER) &&
                    (prepLocGrandChild.index() != -1);

            if (spaceTimeIndicesCheck || spaceTimeIndicesCheckPrep) {
                ObjectArrayList<IndexedWord> spaceTimeWordList = new ObjectArrayList<>();

                // Get prep or not prep cases
                if (gr == EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER) {
                    for (IndexedWord w: prepChildren) {
                        spaceTimeWordList = this.getSpaceTimeWordsList(sg, child, w, t);
                    }
                } else {
                    spaceTimeWordList = this.getSpaceTimeWordsList(sg, head, child, t);
                }

                if (spaceTimeWordList.isEmpty()) {
                    return;
                }
                else {
                    // Annotate and drop
                    if (t == SpaTeCore.type.TIME) {
                        this.tempmod = head;
                        this.setTime(new Time(spaceTimeWordList));
                        this.temporalflag = true;
                        this.wordList.removeAll(this.getTime().getCoreTemporalWords());
                    } else if (t == SpaTeCore.type.SPACE){
                        this.spacemod = head;
                        this.setSpace(new Space(spaceTimeWordList));
                        if (gr == EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER) {
                            this.space.setPredicate(child);
                            this.wordList.remove(child);
                        }
                        this.spatialflag = true;
                        this.wordList.removeAll(this.getSpace().getCoreSpatialWords());
                    }
                }
            }
        }
    }

    /**
     * Given a governor and dependent node, get the list of words (which are its descendants) for space/time annotations
     * @param sg semantic graph of the sentence
     * @param gov the governor node (the one modified by time/space)
     * @param dep the head node in the graph from which the list is created
     * @param t either TIME or SPACE
     * @return list of space/time words
     */
    public static ObjectArrayList<IndexedWord> getSpaceTimeWordsList(SemanticGraph sg, IndexedWord gov, IndexedWord dep, SpaTeCore.type t) {
        Collection<GrammaticalRelation> tabuRels = getTabuRels();
        Set<IndexedWord> spaceTimeWordsSet = SemanticGraphUtils.descendantsTabuRelns(sg, dep, tabuRels);
        spaceTimeWordsSet.add(dep);
        spaceTimeWordsSet.addAll(WordCollectionUtils.getChainedWords(dep, new ObjectArrayList<>(sg.getChildren(gov))));

        ObjectArrayList<IndexedWord> wordsList = WordCollectionUtils.getSortedWords(spaceTimeWordsSet);
        ObjectArrayList<IndexedWord> ignoreWords = new ObjectArrayList<>();

        // Ignore some words if it's temporal
        if (t == SpaTeCore.type.TIME) {
            for (IndexedWord word : wordsList) {
                if (Time.ignoreTempWord(word)) {
                    ignoreWords.add(word);
                }

            }
        }
        wordsList.removeAll(ignoreWords);

        return wordsList;
    }

    /**
     * @return Relations to be excluded from the subgraph (and its descendants)
     */
    private static Collection<GrammaticalRelation> getTabuRels() {
        Collection<GrammaticalRelation> tabuRels = new ArrayList<>();
        tabuRels.add(EnglishGrammaticalRelations.RELATIVE_CLAUSE_MODIFIER);
        tabuRels.add(EnglishGrammaticalRelations.PUNCTUATION);
        tabuRels.add(EnglishGrammaticalRelations.APPOSITIONAL_MODIFIER);
        tabuRels.add(EnglishGrammaticalRelations.valueOf("dep"));
        tabuRels.add(EnglishGrammaticalRelations.COORDINATION);
        tabuRels.add(EnglishGrammaticalRelations.CONJUNCT);
        tabuRels.add(EnglishGrammaticalRelations.VERBAL_MODIFIER);
        return tabuRels;
    }

    public boolean isOneNER() {
        return WordCollectionUtils.isOneNER(this.wordList);
    }

    public void removeWord(IndexedWord w) {
        this.wordList.remove(w);
    }
    public boolean removeAll(ObjectArrayList<IndexedWord> words) {
        return this.wordList.removeAll(words);
    }
    public boolean containsAll(ObjectArrayList<IndexedWord> words) {
        return this.wordList.containsAll(words);
    }
}
