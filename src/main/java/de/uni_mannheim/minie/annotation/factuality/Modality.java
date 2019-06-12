package de.uni_mannheim.minie.annotation.factuality;

import java.io.IOException;
import java.util.List;

import de.uni_mannheim.clausie.phrase.Phrase;
import de.uni_mannheim.constant.CHARACTER;
import de.uni_mannheim.constant.POS_TAG;
import de.uni_mannheim.constant.REGEX;
import de.uni_mannheim.constant.SEPARATOR;
import de.uni_mannheim.utils.Dictionary;

import de.uni_mannheim.utils.coreNLP.WordUtils;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.ling.tokensregex.TokenSequenceMatcher;
import edu.stanford.nlp.ling.tokensregex.TokenSequencePattern;
import edu.stanford.nlp.util.CoreMap;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;


/**
 * Annotation for modality
 *
 * @author Kiril Gashteovski
 */
public class Modality {
    /** Annotations for modality, can be just "CERTAINTY" or "POSSIBILITY" */
    public enum Type {CERTAINTY, POSSIBILITY}
    
    /** Strings expressing modality types **/
    public static String ST_CERTAINTY = "CERTAINTY";
    public static String ST_CT = "CT";
    public static String ST_POSSIBILITY = "POSSIBILITY";
    public static String ST_PS = "PS";
    
    /** List of possibility words and edges (if found any) **/
    private ObjectOpenHashSet<IndexedWord> possibilityWords; 

    /** List of certainty words and edges (if found any) **/
    private ObjectOpenHashSet<IndexedWord> certaintyWords;

    /** Modality type **/
    private Modality.Type modalityType;
    
    /** A set of all words expressing possibility **/
    public static Dictionary POSS_WORDS;
    static {
        try {
            POSS_WORDS = new Dictionary("/minie-resources/poss-words.dict");
        } catch (IOException e) {
            throw new Error(e);
        } 
    }
    
    
    /** A set of negative possibility words **/
    public static Dictionary NEG_POSS_WORDS;
    static {
        try {
            NEG_POSS_WORDS = new Dictionary("/minie-resources/poss-neg-words.dict");
        } catch (IOException e) {
            throw new Error(e);
        } 
    }
    
    /** A set of adjectives expressing possibility **/
    public static Dictionary POSS_ADJ;
    static {
        try {
            POSS_ADJ = new Dictionary("/minie-resources/poss-adj.dict");
        } catch (IOException e) {
            throw new Error(e);
        } 
    }
    
    /** A set of certainty adverbs **/
    public static Dictionary CERTAINTY_WORDS;
    static {
        try {
            CERTAINTY_WORDS = new Dictionary("/minie-resources/certainty-words.dict");
        } catch (IOException e) {
            throw new Error(e);
        } 
    }
    
    /** A set of adverbs expressing possibility **/
    public static Dictionary POSS_ADVERBS;
    static {
        try {
            POSS_ADVERBS = new Dictionary("/minie-resources/poss-adverbs.dict");
        } catch (IOException e) {
            throw new Error(e);
        } 
    }
    
    /** A set of possibility modals **/ 
    public static Dictionary MODAL_POSSIBILITY;
    static {
        try {
            MODAL_POSSIBILITY = new Dictionary("/minie-resources/poss-modal.dict");
        } catch (IOException e) {
            throw new Error(e);
        } 
    }
    
    /** A set of certainty verbs **/
    public static Dictionary VERB_CERTAINTY;
    static {
        try {
            VERB_CERTAINTY = new Dictionary("/minie-resources/certainty-verbs.dict");
        } catch (IOException e) {
            throw new Error(e);
        } 
    }
    
    /** A set of possibility verbs **/
    public static Dictionary VERB_POSSIBILITY;
    static {
        try {
            VERB_POSSIBILITY = new Dictionary("/minie-resources/poss-verbs.dict");
        } catch (IOException e) {
            throw new Error(e);
        } 
    }
    
    /** Default constructor. Assumes 'certainty' modality type, creates empty lists of poss/cert words and edges **/
    public Modality(){
        this.modalityType = Modality.Type.CERTAINTY;
        this.possibilityWords = new ObjectOpenHashSet<>();
        this.certaintyWords = new ObjectOpenHashSet<>();
    }
    /**
     * Copy constructor
     * @param m: modality object
     */
    public Modality(Modality m){
        this.modalityType = m.getModalityType();
        this.possibilityWords = m.getPossibilityWords();
        this.certaintyWords = m.getCertaintyWords();
    }
    /**
     * Given the modality type, the list of edges and words are empty lists 
     * @param t: Modality type
     */
    public Modality(Modality.Type t){
        this.modalityType = t;
        this.possibilityWords = new ObjectOpenHashSet<>();
        this.certaintyWords = new ObjectOpenHashSet<>();
    }
    /**
     * Constructor with given the modality type and list of possibility words. The certainty lists of words are empty.
     * @param t: modality type
     * @param possWords: possibility words
     */
    public Modality(Modality.Type t, ObjectOpenHashSet<IndexedWord> possWords){
        this.modalityType = t;
        this.possibilityWords = possWords;
        this.certaintyWords = new ObjectOpenHashSet<>();
    }
    
    /** Getters **/
    public Modality.Type getModalityType(){
        return this.modalityType;
    }
    public ObjectOpenHashSet<IndexedWord> getCertaintyWords(){
        return this.certaintyWords;
    }
    public ObjectOpenHashSet<IndexedWord> getPossibilityWords(){
        return this.possibilityWords;
    }

    /** Setters **/
    public void setModalityType(Modality.Type t){
        this.modalityType = t;
    }

    /** Adding elements to lists **/
    public void addCertaintyWords(ObjectArrayList<IndexedWord> words) {
        this.certaintyWords.addAll(words);
    }
    public void addPossibilityWords(ObjectArrayList<IndexedWord> words){
        this.possibilityWords.addAll(words);
    }
    
    /**
     * Given a phrase, get the modality for the phrase
     * @param relation: a phrase (relation)
     * @return Modality of the phrase
     */
    public static Modality getModality(Phrase relation){
        Modality mod = new Modality();
        ObjectArrayList<IndexedWord> certaintyWords = new ObjectArrayList<>();
        ObjectArrayList<IndexedWord> possibilityWords = new ObjectArrayList<>();
        
        // Add words to the possibility/certainty
        for (int i = 0; i < relation.getWordList().size(); i++){
            if (WordUtils.isAdverb(relation.getWordList().get(i))){
                // Check for possibility adverbs
                if (Modality.POSS_ADVERBS.contains(relation.getWordList().get(i).lemma())){
                    possibilityWords.add(relation.getWordList().get(i));
                }
                // Check for certainty adverbs
                else if (Modality.CERTAINTY_WORDS.contains(relation.getWordList().get(i).lemma())){
                    certaintyWords.add(relation.getWordList().get(i));
                }
            }
            // Check for possibility adjectives
            else if (WordUtils.isAdj(relation.getWordList().get(i))){
                if (Modality.POSS_ADJ.contains(relation.getWordList().get(i).lemma())){
                    possibilityWords.add(relation.getWordList().get(i));
                }
            }
            // Check for modals (possibility and certainty)
            else if (relation.getWordList().get(i).tag().equals(POS_TAG.MD)){
                if (Modality.MODAL_POSSIBILITY.contains(relation.getWordList().get(i).lemma())){
                    possibilityWords.add(relation.getWordList().get(i));
                }
            }
        }
        
        // Check for modality verb phrases
        TokenSequencePattern tPattern = TokenSequencePattern.compile(REGEX.T_POSS_VP);
        TokenSequenceMatcher tMatcher = tPattern.getMatcher(relation.getWordCoreLabelList());
        
        while (tMatcher.find()){         
            List<CoreMap> match = tMatcher.groupNodes();
            IndexedWord firstWord = new IndexedWord(new CoreLabel(match.get(0)));
            if (firstWord.index() != relation.getWordList().get(0).index())
                break;
            IndexedWord w;
            for (CoreMap aMatch : match) {
                w = new IndexedWord(new CoreLabel(aMatch));
                possibilityWords.add(w);
                if (w.tag().equals(POS_TAG.TO))
                    break;
            }
        }
        
        // If there are both possibility and certainty, certainty get subsummed by possibility
        if (!certaintyWords.isEmpty() && !possibilityWords.isEmpty()){
            mod.setModalityType(Modality.Type.POSSIBILITY);
            mod.addPossibilityWords(possibilityWords);
            mod.addPossibilityWords(certaintyWords);
        }
        else if (!certaintyWords.isEmpty()){
            mod.setModalityType(Modality.Type.CERTAINTY);
            mod.addCertaintyWords(certaintyWords);
        }
        else if (!possibilityWords.isEmpty()) {
            mod.setModalityType(Modality.Type.POSSIBILITY);
            mod.addPossibilityWords(possibilityWords);
        }
        else {
            mod.setModalityType(Type.CERTAINTY);
        }
        
        return mod;
    }
    
    /** Given a modality object, convert it into a string */
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(CHARACTER.LPARENTHESIS);
        if (this.modalityType == Modality.Type.POSSIBILITY)
            sb.append(Modality.ST_POSSIBILITY);
        else
            sb.append(Modality.ST_CERTAINTY);
        sb.append(CHARACTER.COMMA);
        sb.append(SEPARATOR.SPACE);
        
        if (this.modalityType == Modality.Type.POSSIBILITY){
            for (IndexedWord w: this.possibilityWords) {
                sb.append(w.word());
                sb.append(CHARACTER.COMMA);
                sb.append(SEPARATOR.SPACE);
            }
        }
        
        sb.append(CHARACTER.RPARENTHESIS);
        return sb.toString().trim();
    }
}
