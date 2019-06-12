package de.uni_mannheim.minie.confidence;

import de.uni_mannheim.clausie.ClausIE;
import de.uni_mannheim.clausie.Options;
import de.uni_mannheim.clausie.clause.Clause;
import de.uni_mannheim.clausie.constituent.Constituent;
import de.uni_mannheim.minie.MinIE;
import de.uni_mannheim.minie.annotation.AnnotatedPhrase;
import de.uni_mannheim.minie.annotation.AnnotatedProposition;
import de.uni_mannheim.utils.Dictionary;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import edu.stanford.nlp.trees.GrammaticalRelation;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * @author Sebastian Wanner
 * @author Kiril Gashteovski
 */
public class FeatureExtractor {
    private static final Logger logger = LoggerFactory.getLogger(FeatureExtractor.class);
    private AnnotatedProposition proposition;
    private SemanticGraph semGraph;
    private ObjectArrayList<IndexedWord> sentence;
    private ObjectArrayList<AnnotatedProposition> dictModePropositions;
    private ObjectArrayList<AnnotatedProposition> aggressiveModePropositions;
    private Clause clause;
    private ClausIE clausIE;
    private MinIE minie;
    private Dictionary collocations;

    public FeatureExtractor() {
        this.proposition  = null;
        this.semGraph = new SemanticGraph();
        this.sentence = new ObjectArrayList<>();
        this.clause = null;

        clausIE = new ClausIE();
    }

    public void clear() {
        this.proposition = null;
        this.semGraph = null;
        this.clause = null;
    }

    public void clearPropositionsDictMode() {
        this.dictModePropositions = null;
    }

    public void clearPropositionsAggressiveMode() {
        this.aggressiveModePropositions = null;
    }


    /**
     * Phrase in the Extraction have a Dep Relation
     * @return 1= true; 0 = false;
     */
    public int extractionContainsDepRelation() {
        for (AnnotatedPhrase phrase : this.proposition.getTriple()) {
            for (IndexedWord word : phrase.getWordList()) {
                if (!this.semGraph.containsVertex(word)) {
                    return 0;
                }

                if (this.semGraph.hasChildWithReln(word, GrammaticalRelation.DEPENDENT)) {
                    return 1;
                }

                if (this.semGraph.hasParentWithReln(word, GrammaticalRelation.DEPENDENT)) {
                    return 1;
                }
            }
        }

        return 0;
    }

    /**
     * Does the extraction have the same order
     * @return 1= true; 0 = false;
     */
    public int orderOfExtraction() {
        int position = 0;

        for(AnnotatedPhrase phrase : this.proposition.getTriple()) {
            for(IndexedWord word : phrase.getWordList()) {
                if(word.value().equals("be") || word.value().equals("has") || word.value().equals("is") ) {
                    continue;
                }

                if(word.value().equals("female") || word.value().equals("male")) {
                    continue;
                }

                if(word.value().contains("QUANT")) {
                    continue;
                }

                if (position == 0) {
                    position = word.beginPosition();
                }

                if(position > word.beginPosition()) {
                    return 0;
                }

                position = word.beginPosition();
            }
        }

        return 1;
    }

    /**
     * Is there quantities or dates in the triple
     * @return 1= true; 0 = false;
     */
    public int extractsQuantity() {
        if(this.proposition.getObject().getHeadWord().ner() == null) {
            return 0;
        }

        if(this.proposition.getObject().getHeadWord().ner().equals("DATE") && this.proposition.getRelation().toString().equals("is") ) {
            return 1;
        }

        if(this.proposition.getObject().getWordList().get(0).tag().equals("QUANTITY") && this.proposition.getRelation().toString().equals("is") ) {
            return 1;
        }

        return 0;
    }

    /**
     * Is there a infinite verb in subject
     * @return 1= true; 0 = false;
     */
    public int containsInfiniteVerbInSubject() {
        ObjectArrayList<IndexedWord> wordList = this.proposition.getSubject().getWordList();

        if (wordList == null) {
            return 0;
        }

        for (int i = 0; i < wordList.size() - 1; i++) {
            if(wordList.get(i).tag().contains("TO") && wordList.get(i+1).tag().contains("VB")) {
                return 1;
            }
        }
        return 0;
    }

    /**
     * Is there a infinite verb in relation
     * @return 1= true; 0 = false;
     */
    public int containsInfiniteVerbInRelation() {
        ObjectArrayList<IndexedWord> wordList = this.proposition.getRelation().getWordList();

        if (wordList == null) {
            return 0;
        }

        for (int i = 0; i < wordList.size() - 1; i++) {
            if(wordList.size() < 2) {
                continue;
            }
            if(wordList.get(i).word().contains("be") && wordList.get(i+1).tag().contains("VB")) {
                return 1;
            }
        }
        return 0;
    }


    /**
     * contains possesive relation
     * @return 1= true; 0 = false;
     */
    public int containsPossRelation() {
        IndexedWord govenor = new IndexedWord();
        IndexedWord dependent = new IndexedWord();

        for (IndexedWord word : this.sentence) {
            if (this.semGraph.hasParentWithReln(word, EnglishGrammaticalRelations.POSSESSION_MODIFIER)) {
                govenor = semGraph.getParent(word);
                dependent = word;
            }
        }

        if (govenor.value() == null && dependent.value() == null) {
            return 0;
        }

        if(this.proposition.getSubject().toString().contains(dependent.value())
                && this.proposition.getObject().toString().contains(govenor.value())) {
            return 1;
        }

        return 0;
    }

    /**
     * triple contains gerund
     * @return 1= true; 0 = false;
     */
    public int containsGerund() {
        for (int i = 0; i < this.sentence.size() - 1; i++) {
            if (this.sentence.get(i).tag().contains(",") && this.sentence.get(i + 1).tag().contains("VBG")) {
                if (this.proposition.wordsToString().contains(this.sentence.get(i + 1).value())) {
                    return 1;
                }
            }
        }

        return 0;
    }


    /**
     * Whether both conjuncts head words have the same POSTag
     * @return 1=true, 0=false
     */
    public int conjPOSTagSubj() {
        IndexedWord govenor = new IndexedWord();
        IndexedWord dependent = new IndexedWord();
        IndexedWord word;

        if (clause == null) {
            return  0;
        }

        if (this.proposition.getSubject().isProcessedConjunction()) {
            word = this.proposition.getSubject().getConjWord();
            if (!semGraph.containsVertex(word)) {
                return  0;
            }
            if (this.semGraph.hasParentWithReln(word, EnglishGrammaticalRelations.CONJUNCT)) {
                govenor = semGraph.getParent(word);
                dependent = word;
            }
        }
        if (govenor.value() == null && dependent.value() == null) {
            //return -1;
            return  0;
        }
        if(dependent.tag().equals(govenor.tag())) {
            return 1;
        }
        else return 0;
    }



    /**
     * Relation as a whole String in the sentence
     * @return 1= true; 0 = false;
     */
    public int relationAsWholeString() {
        String sent = "";

        for(IndexedWord word : sentence) {
            sent = sent.concat(word.originalText() + " ");
        }

        if(sent.contains(" " + this.proposition.getRelation().getWords() + "")) {
            return 1;
        }
        return 0;
    }


    /**
     * counts the length of extraction
     * @return 1= true; 0 = false;
     */
    public int lengthOfExtraction() {
        int length = 0;

        for(IndexedWord ignored : this.proposition.getSubject().getWordList()) {
            length = length  + 1;
        }

        for(IndexedWord ignored : this.proposition.getRelation().getWordList()) {
            length = length  + 1;
        }

        for(IndexedWord ignored : this.proposition.getObject().getWordList()) {
            length = length  + 1;
        }
        return length;
    }



    /**
     * check whether optional adverbials are in the extraction
     * @return 1=yes; 0=no
     */
    public int droppedAllOptionalAdverbial(){
        int status = 0;
        if (this.clause == null){
            //return -1;
            return  0;
        }

        if (clause.getAdverbialInds().size() == 0) {
            return 0;
        }

        for (int i: clause.getAdverbialInds()) {
            if (clause.getConstituentStatus(i, new Options()).toString().equals("OPTIONAL")) {
                Constituent optAdverbial = clause.getConstituents().get(i);
                if (!this.proposition.toString().contains(optAdverbial.rootString())) {
                    status = 1;
                } else{
                    status = 0;
                }
            }
        }

        return status;
    }

    /**
     * detected Clause Type of Clausie
     * @return one of this types (SV, SVC, SVA, SVO, SVOO, SVOC, SVOA, EXISTENTIAL, UNKNOWN)
     * 			or NOTYPE for implicit extractions
     */
    public String getClauseType() {
        if (clause != null) {
            return this.clause.getType().toString();
        }
        return "NOTYPE";
    }


    /**
     * checked for dropped Prep
     * @return 1= true; 0 = false;
     */
    public int droppedPrep() {
        IndexedWord govenor = new IndexedWord();
        IndexedWord dependent = new IndexedWord();

        if (this.clause == null) {
            //return -1;
            return  0;
        }


        for (IndexedWord word : this.proposition.getRelation().getWordList()) {
            if (!semGraph.containsVertex(word)) {
                continue;
            }

            if (this.semGraph.hasParentWithReln(word, EnglishGrammaticalRelations.PREPOSITIONAL_OBJECT)) {
                govenor = semGraph.getParent(word);

                if (!this.proposition.getRelation().getWordList().contains(govenor)) {
                    return 1;
                }
            }
        }


        for (IndexedWord word2 : this.proposition.getObject().getWordList()) {
            if (!semGraph.containsVertex(word2)) {
                continue;
            }

            if (this.semGraph.hasParentWithReln(word2, EnglishGrammaticalRelations.PREPOSITIONAL_OBJECT)) {
                govenor = semGraph.getParent(word2);
                dependent = word2;

                if ((!this.proposition.getRelation().getWordList().contains(govenor)) &&
                        (!this.proposition.getObject().getWordList().contains(govenor))) {

                    if(govenor.beginPosition() < dependent.beginPosition()) {
                        return 1;
                    }
                }

            }
        }
        return 0;
    }


    /**
     * check if the extractions occurs in DictMode
     * @return  1=true, 0=false
     */
    public int occursInDictMode() throws IOException {
        if (this.minie == null) {
            this.minie = new MinIE();

            String [] filenames = new String [] {
                    "/minie-resources/wn-mwe.txt",
                    "/minie-resources/wiktionary-mw-titles.txt",
            };

            collocations = new Dictionary (filenames);
        }

        if (this.dictModePropositions == null) {
            minie.clear();
            minie.setSemanticGraph(this.semGraph);
            minie.setPropositions(clausIE);
            minie.annotatePolarity();
            minie.annotateModality();
            minie.minimizeDictionaryMode(collocations.words());
            minie.removeDuplicates();
            this.dictModePropositions = minie.getPropositions();
        }

        for (AnnotatedProposition aprop: this.dictModePropositions) {
            if (aprop.wordsToString().equals(this.proposition.wordsToString())) {
                return 1;
            }
        }

        //        for( AnnotatedProposition aprop: minie.getPropositions()) {
        //            if(aprop.wordsToString().equals(this.proposition.wordsToString())) {
        //                return 1;
        //            }
        //        }
        return 0;
    }

    /**
     * check if the extractions occurs in AggressiveMode
     * @return  1=true, 0=false
     */
    public int occursInAggressiveMode() {
        if (this.minie == null) {
            this.minie = new MinIE();
        }

        if (this.aggressiveModePropositions == null) {
            minie.clear();
            minie.setSemanticGraph(this.semGraph);
            minie.setPropositions(clausIE);
            minie.annotatePolarity();
            minie.annotateModality();
            minie.minimizeAggressiveMode();
            minie.removeDuplicates();
            this.aggressiveModePropositions = minie.getPropositions();
        }

        for (AnnotatedProposition aprop: this.aggressiveModePropositions) {
            if (aprop.wordsToString().equals(this.proposition.wordsToString())) {
                return 1;
            }
        }

        //        for( AnnotatedProposition aprop: minie.getPropositions()) {
        //            if(aprop.wordsToString().equals(this.proposition.wordsToString())) {
        //                return 1;
        //            }
        //        }
        return 0;
    }

    /**
     * checks if subject is created by Conjunction
     * @return  1=true, 0=false
     */
    public int processedConjunctionSubject() {
        if (this.proposition.getSubject().isProcessedConjunction()) {
            return 1;
        }

        return 0;
    }

    /**
     * checks if relation is created by Conjunction
     * using Safe Mode minimazition
     * @return  1=true, 0=false
     */
    public int processedConjunctionRelation() {
        if (this.proposition.getRelation().isProcessedConjunction()) {
            return 1;
        }
        return 0;
    }

    /**
     * check if object is created by Conjunction
     * using Safe Mode minimazition
     * @return  1=true, 0=false
     */
    public int processedConjunctionObject() {
        if (this.proposition.getObject().isProcessedConjunction()) {
            return 1;
        }
        return 0;
    }

    /**
     * calculate the relative distance of subject and object
     * @return  relative distance
     */

    public int ObjBeforeSubj() {
        int locationSubj = 0;
        int locationObj = 0;
        int distance;

        if (this.proposition.getObject().getWordList().size()==0) {
            return 0;
        }

        for (int i = 0; i<this.sentence.size(); i++) {
            if (this.sentence.get(i).equals(this.proposition.getSubject().getHeadWord())) {
                locationSubj = i;
            }

            if (this.sentence.get(i).equals(this.proposition.getObject().getHeadWord()) &&
                    this.proposition.getObject().getWordList().contains(this.proposition.getObject().getHeadWord())) {
                locationObj = i;
            }
        }

        if (locationObj==0) {
            IndexedWord currentWord = new IndexedWord();
            IndexedWord newWord;
            List<IndexedWord> pathNewWord;
            List<IndexedWord> pathCurrentWord = new ArrayList<>();

            for (int i = 0; i<this.proposition.getObject().getWordList().size(); i++) {
                if (!this.semGraph.containsVertex(this.proposition.getObject().getWordList().get(i))) {
                    return 0;
                }

                if (pathCurrentWord.size() == 0) {
                    pathCurrentWord = this.semGraph.getPathToRoot(this.proposition.getObject().getWordList().get(0));
                    currentWord = this.proposition.getObject().getWordList().get(0);
                }

                pathNewWord = this.semGraph.getPathToRoot(this.proposition.getObject().getWordList().get(i));
                newWord = this.proposition.getObject().getWordList().get(i);

                if (pathCurrentWord.size() > pathNewWord.size()) {
                    pathCurrentWord = pathNewWord;
                    currentWord = newWord;
                }
            }

            for (int i = 0; i<this.sentence.size(); i++) {

                if (this.sentence.get(i).equals(currentWord)){
                    locationObj = i;
                }
            }
        }
        distance = locationObj - locationSubj;

        if (distance < 0) {
            return 1;
        } else {
            return 0;
        }
    }


    /**
     * check if the relation is frequent in the dictionary
     * @return  0=false, 1=true
     */
    public int isRelFrequent100k() throws IOException {
        Map<String, Double> frequentPatterns = new HashMap<>();

        String filename = "/minie-resources/all-POSRelPatternsFreq100000.counts.sorted";
        InputStream is = this.getClass().getResource(filename).openStream();
        DataInput data = new DataInputStream(is);
        String line;
        while ((line = data.readLine()) != null) {
            String parts[] = line.split("\t");
            frequentPatterns.put(parts[0], Double.parseDouble(parts[1]));
        }

        //Dictionary frequentPatterns = new Dictionary(filenames);
        StringBuilder pattern = new StringBuilder();

        for (IndexedWord word : this.proposition.getRelation().getWordList()) {
            pattern.append(word.tag()).append(" ");
        }
        pattern = new StringBuilder(pattern.toString().trim());

        if (frequentPatterns.containsKey(pattern.toString().trim())) {
            return 1;
        }

        return 0;
    }


    /**
     * length of sentence
     * @return number of characters without whitespace
     */
    public int lengthOfSentence() {
        int length = 0;

        for(IndexedWord ignored : sentence) {
            length = length  + 1;
        }

        return length;
    }

    /**
     * containsTimeAnnotation
     * @return 1 = contains time annotation , 0 = do not contain time annotation
     */
    public int containsTime() {
        if(this.proposition.getTime().isEmpty()) {
            return 0;
        }
        return 1;
    }

    /**
     * containsTimeAnnotation
     * @return 1 = contains time annotation , 0 = do not contain time annotation
     */
    public int containsSpace() {
        if(this.proposition.getSpace().isEmpty()) {
            return 0;
        }
        return 1;
    }

    /**
     * create all features of the triple and save it in a map object for the prediction model
     * @return hashmap containing all features
     */

    public Map getFeaturesForPredictionModel() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("Length", (double) this.lengthOfSentence());
        map.put("LengthofExtraction", (double) this.lengthOfExtraction());
        map.put("ClauseType", this.getClauseType());
        map.put("droppedAllOptionalAdverbial",(double) this.droppedAllOptionalAdverbial());
        map.put("droppedPrep", (double)this.droppedPrep());
        map.put("RelWholeString",(double) this.relationAsWholeString());
        map.put("POSTagSubj", (double) this.conjPOSTagSubj());
        map.put("Poss", (double) this.containsPossRelation());
        map.put("Gerund", (double) this.containsGerund());
        map.put("InfiniteVerbSubj", (double) this.containsInfiniteVerbInSubject());
        map.put("InfiniteVerbRel", (double) this.containsInfiniteVerbInRelation());
        map.put("OrderOfExtraction", (double) this.orderOfExtraction());
        map.put("ExtractionContainDep", (double) this.extractionContainsDepRelation());
        map.put("ProcessedConjSubj", (double) this.processedConjunctionSubject());
        map.put("ProcessedConjRel", (double) this.processedConjunctionRelation());
        map.put("ProcessedConjObj", (double) this.processedConjunctionObject());
        map.put("ObjBeforeSubj", (double) this.ObjBeforeSubj());
        map.put("occursInDict", (double) this.occursInDictMode());
        map.put("occursInAggressive", (double) this.occursInAggressiveMode());
        map.put("isRelFreq100k", (double) this.isRelFrequent100k());
        map.put("extractsQuantity", (double) this.extractsQuantity());
        map.put("containsTime", (double) this.containsTime());
        map.put("containsSpace", (double) this.containsSpace());
        return map;
    }

    /**
     * create all features from a sentence
     * @param del: delimeter
     * @return all features as a string seperated with a delimeter
     */

    public String getFeatureExecutionTime(String del) throws IOException {
        long startTime;
        long stopTime;

        StringBuilder builder = new StringBuilder();

        Map<String, Object> map = new HashMap<>();

        startTime = System.currentTimeMillis();
        map.put("Length", (double) this.lengthOfSentence());
        stopTime = System.currentTimeMillis();
        //System.out.println("LengthOfSentence: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("LengthofExtraction", (double) this.lengthOfExtraction());
        stopTime = System.currentTimeMillis();
        //System.out.println("LengthOfExtraction: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("ClauseType", this.getClauseType());
        stopTime = System.currentTimeMillis();
        //System.out.println("getClauseType: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);


        startTime = System.currentTimeMillis();
        map.put("droppedAllOptionalAdverbial",(double) this.droppedAllOptionalAdverbial());
        stopTime = System.currentTimeMillis();
        //System.out.println("droppedAllOptionalAdverbial: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("droppedPrep", (double)this.droppedPrep());
        stopTime = System.currentTimeMillis();
        //System.out.println("droppedPrep: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("RelWholeString",(double) this.relationAsWholeString());
        stopTime = System.currentTimeMillis();
        //System.out.println("relationAsWholeString: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("POSTagSubj", (double) this.conjPOSTagSubj());
        stopTime = System.currentTimeMillis();
        //System.out.println("conjPosTagSubj: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("Poss", (double) this.containsPossRelation());
        stopTime = System.currentTimeMillis();
        //System.out.println("containsPossRelation: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("Gerund", (double) this.containsGerund());
        stopTime = System.currentTimeMillis();
        //System.out.println("containsGerund: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("InfiniteVerbSubj", (double) this.containsInfiniteVerbInSubject());
        stopTime = System.currentTimeMillis();
        //System.out.println("containsInfiniteVerbInSubject: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("InfiniteVerbRel", (double) this.containsInfiniteVerbInRelation());
        stopTime = System.currentTimeMillis();
        //System.out.println("containsInfiniteVerbInRelation: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("OrderOfExtraction", (double) this.orderOfExtraction());
        stopTime = System.currentTimeMillis();
        //System.out.println("orderOfExtraction: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("ExtractionContainDep", (double) this.extractionContainsDepRelation());
        stopTime = System.currentTimeMillis();
        //System.out.println("extractionContainsDepRelation: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("ProcessedConjSubj", (double) this.processedConjunctionSubject());
        stopTime = System.currentTimeMillis();
        //System.out.println("processedConjunctionSubject: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("ProcessedConjRel", (double) this.processedConjunctionRelation());
        stopTime = System.currentTimeMillis();
        //System.out.println("processedConjunctionRelation: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("ProcessedConjObj", (double) this.processedConjunctionObject());
        stopTime = System.currentTimeMillis();
        //System.out.println("processConjunctionObject: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("ObjBeforeSubj", (double) this.ObjBeforeSubj());
        stopTime = System.currentTimeMillis();
        //System.out.println("ObjBeforeSubj: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("occursInDict", (double) this.occursInDictMode());
        stopTime = System.currentTimeMillis();
        //System.out.println("occursInDictMode: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("occursInAggressive", (double) this.occursInAggressiveMode());
        stopTime = System.currentTimeMillis();
        //System.out.println("occursInAggressiveMode: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("isRelFreq100k", (double) this.isRelFrequent100k());
        stopTime = System.currentTimeMillis();
        //System.out.println("isRelFreq100k: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        map.put("extractsQuantity", (double) this.extractsQuantity());
        stopTime = System.currentTimeMillis();
        //System.out.println("extractsQuantity: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);


        startTime = System.currentTimeMillis();
        map.put("ContainsTime", (double) this.containsTime());
        stopTime = System.currentTimeMillis();
        //System.out.println("ContainsTime: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime)).append(del);

        startTime = System.currentTimeMillis();
        this.containsSpace();
        stopTime = System.currentTimeMillis();
        //System.out.println("ContainsSpace: " + Long.toString(stopTime - startTime));
        builder.append(Long.toString(stopTime - startTime));

        return builder.toString();
        //return map;
    }

    /**
     * create all features from a sentence
     * @param del: delimeter
     * @return all features as a string seperated with a delimeter
     */

    public String getFeaturesWithoutFormatting(String del) throws Exception{
        StringBuilder builder = new StringBuilder();
        builder.append(this.lengthOfSentence()).append(del);
        builder.append(this.lengthOfExtraction()).append(del);
        builder.append(this.getClauseType()).append(del);
        builder.append(this.droppedAllOptionalAdverbial()).append(del);
        builder.append(this.droppedPrep()).append(del);
        builder.append(this.relationAsWholeString()).append(del);
        builder.append(this.conjPOSTagSubj()).append(del);
        builder.append(this.containsPossRelation()).append(del);
        builder.append(this.containsGerund()).append(del);
        builder.append(this.extractsQuantity()).append(del);
        builder.append(this.containsInfiniteVerbInSubject()).append(del);
        builder.append(this.containsInfiniteVerbInRelation()).append(del);
        builder.append(this.orderOfExtraction()).append(del);
        builder.append(this.extractionContainsDepRelation()).append(del);
        builder.append(this.processedConjunctionSubject()).append(del);
        builder.append(this.processedConjunctionRelation()).append(del);
        builder.append(this.processedConjunctionObject()).append(del);
        builder.append(this.ObjBeforeSubj()).append(del);
        builder.append(this.occursInDictMode()).append(del);
        builder.append(this.occursInAggressiveMode()).append(del);
        builder.append(this.containsTime()).append(del);
        builder.append(this.containsSpace()).append(del);
        builder.append(this.isRelFrequent100k());
        System.out.print(builder.toString() + "\n");
        return builder.toString();
    }


    public AnnotatedProposition getProposition() {
        return proposition;
    }

    public void setProposition(AnnotatedProposition proposition) {
        this.proposition = proposition;
    }

    public void setSemGraph(SemanticGraph semGraph) {
        this.semGraph = semGraph;
    }

    public ObjectArrayList<IndexedWord> getSentence() {
        return sentence;
    }

    public void setSentence(ObjectArrayList<IndexedWord> sentence) {
        this.sentence = sentence;
    }

    public Clause getClause() {
        return clause;
    }

    public void setClause(Clause clause) {
        this.clause = clause;
    }

    public ClausIE getClausIE() {
        return clausIE;
    }

    public void setClausIE(ClausIE clausIE) {
        this.clausIE = clausIE;
    }
}


