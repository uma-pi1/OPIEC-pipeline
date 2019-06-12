package de.uni_mannheim.minie.proposition;

import de.uni_mannheim.constant.*;
import de.uni_mannheim.minie.annotation.AnnotatedPhrase;
import de.uni_mannheim.minie.annotation.AnnotatedProposition;
import de.uni_mannheim.minie.annotation.Attribution;
import de.uni_mannheim.minie.annotation.factuality.Polarity;
import de.uni_mannheim.utils.coreNLP.CoreNLPUtils;

import de.uni_mannheim.utils.coreNLP.WordCollectionUtils;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.ling.tokensregex.TokenSequenceMatcher;
import edu.stanford.nlp.ling.tokensregex.TokenSequencePattern;
import edu.stanford.nlp.semgraph.SemanticGraph;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import static de.uni_mannheim.utils.minie.Utils.argsNERCheckForBeRel;

/**
 * @author Kiril Gashteovski
 */
public class ImplicitExtractions {
    /** Token sequence pattern **/
    private TokenSequencePattern tPattern;
    /** Token sequence matcher **/
    private TokenSequenceMatcher tMatcher;
    
    /** The sentence as a list of indexed words **/
    private ObjectArrayList<IndexedWord> sentence;
    /** The dependency parse graph of the sentence **/
    private SemanticGraph sentenceSemGraph;
    
    // Annotated phrases for subject, relation and object
    private AnnotatedPhrase subj;
    private AnnotatedPhrase rel;
    private AnnotatedPhrase obj;
    
    /** List of annotated propositions **/
    private ObjectArrayList<AnnotatedProposition> propositions;
    
    /** Default constructor with empty elements, given the sentence and the dependency parse graph
     * @param sent: sentence as a list of indexed words
     * @param sg: the dependency parse graph
     */
    public ImplicitExtractions(ObjectArrayList<IndexedWord> sent, SemanticGraph sg) {
        this.subj = new AnnotatedPhrase();
        this.rel = new AnnotatedPhrase();
        this.obj = new AnnotatedPhrase();
        this.sentence = sent.clone();
        this.propositions = new ObjectArrayList<>();
        this.sentenceSemGraph = sg;
    }
    
    /** Set the the relation to a is-a relation **/
    private void setIsARelation() {
        this.rel = new AnnotatedPhrase();
        IndexedWord beWord = new IndexedWord();
        beWord.setWord("is");
        beWord.setOriginalText("is");
        beWord.setTag(POS_TAG.VBZ);
        beWord.setNER(NE_TYPE.NO_NER);
        beWord.setLemma("be");
        beWord.setValue("is");
        beWord.setIndex(-2);
        this.rel.addWordToList(beWord);
        this.rel.setHeadWord(beWord);
    }
    
    /** Generate compound nouns extractions */
    public void generateCompoundNounsExtractions() {
        this.extractPersonIsNPOfOrg();
    }
    
    /** Generate all implicit extractions **/
    public void generateImplicitExtractions() {
        this.generateHypernymExtractions();
        this.generateCompoundNounsExtractions();
        this.generateSurfacePatternExtractions();
    }

    /** Get the hypernym implicit extractions from FINET **/
    public void generateHypernymExtractions() {
        // Make the implicit extractions
        this.extractNounPerson();
        this.extractPersonAmongNP();
        this.extractHearst1();
        this.extractHearst2();
        this.extractHearst2_2();
        this.extractHearst3();
        this.extractHearst4();
        this.extractCityOfLocation();
    }
    
    /** Generate some extractions from TokenRegex patterns **/
    public void generateSurfacePatternExtractions() {
        // Reusable variables
        ObjectArrayList<AnnotatedPhrase> tempProp = new ObjectArrayList<>();
        IndexedWord subjRoot;
        IndexedWord objRoot;
        
        this.tPattern = TokenSequencePattern.compile(REGEX.T_ORG_IN_LOC);
        this.tMatcher = this.tPattern.getMatcher(WordCollectionUtils.getCoreLabelList(this.sentence));
        while (this.tMatcher.find()){
            this.setIsARelation();
            for (IndexedWord w: WordCollectionUtils.toIndexedWordList(this.tMatcher.groupNodes())) {
                if (w.ner().equals(NE_TYPE.ORGANIZATION)) {
                    this.subj.addWordToList(w);
                }
                else if (w.ner().equals(NE_TYPE.LOCATION)) {
                    this.obj.addWordToList(w);
                }
                else if (w.ner().equals(NE_TYPE.NO_NER) && w.tag().equals(POS_TAG.IN)) {
                    this.rel.addWordToList(w);
                }
            }
            subjRoot = CoreNLPUtils.getHeadWord(subj.getWordList(), this.sentenceSemGraph);
            objRoot = CoreNLPUtils.getHeadWord(obj.getWordList(), this.sentenceSemGraph);
            tempProp.add(new AnnotatedPhrase(this.subj.getWordList().clone(), subjRoot));
            tempProp.add(new AnnotatedPhrase(this.rel.getWordList().clone(), this.rel.getHeadWord()));
            tempProp.add(new AnnotatedPhrase(this.obj.getWordList().clone(), objRoot));
            AnnotatedProposition aProp = new AnnotatedProposition(tempProp.clone(), new Attribution());
            aProp.setExtractionType(EXTRACTION_TYPE.IMPL_ORG_IN_LOC);
            this.propositions.add(aProp);
            
            // Clean the variables
            tempProp.clear();
            this.subj.clear();
            this.rel.clear();
            this.obj.clear();
        }
    }
     
    /** If   ORG+ POS? NP PERSON+ => "PERSON" "is NP of" "ORG" (if there are , and or -> make multiple extractions) **/
    private void extractPersonIsNPOfOrg() {
        // Reusable variables
        ObjectArrayList<AnnotatedPhrase> tempProp = new ObjectArrayList<>();
        ObjectArrayList<AnnotatedPhrase> subjects = new ObjectArrayList<>();
        IndexedWord subjRoot;
        IndexedWord objRoot;
        
        this.tPattern = TokenSequencePattern.compile(REGEX.T_ORG_NP_PERSON);
        this.tMatcher = this.tPattern.getMatcher(WordCollectionUtils.getCoreLabelList(this.sentence));
        while (this.tMatcher.find()){    
            // Set the relation to be "is-a" relation
            this.setIsARelation();
            
            for (IndexedWord w: WordCollectionUtils.toIndexedWordList(this.tMatcher.groupNodes())) {
                if (w.ner().equals(NE_TYPE.PERSON))
                    this.subj.addWordToList(w);
                else if (w.ner().equals(NE_TYPE.ORGANIZATION))
                    this.obj.addWordToList(w);
                else if (w.tag().equals(POS_TAG.POS))
                    continue;
                else if (w.lemma().equals(CHARACTER.COMMA) || w.lemma().equals("and") || w.lemma().equals("or")) {
                    subjRoot = CoreNLPUtils.getHeadWord(this.subj.getWordList(), this.sentenceSemGraph);
                    subjects.add(new AnnotatedPhrase(this.subj.getWordList().clone(), subjRoot));
                    this.subj.clear();
                }
                else this.rel.addWordToList(w);
            }
            subjRoot = CoreNLPUtils.getHeadWord(this.subj.getWordList(), this.sentenceSemGraph);
            subjects.add(new AnnotatedPhrase(this.subj.getWordList().clone(), subjRoot));
            objRoot = CoreNLPUtils.getHeadWord(this.obj.getWordList(), this.sentenceSemGraph);
            
            IndexedWord ofWord = this.generatePrepositionOf();
            this.rel.addWordToList(ofWord);
            
            for (AnnotatedPhrase subject: subjects) {
                // Add the subj/rel/obj to the temporary proposition and then to the real propositions
                subjRoot = CoreNLPUtils.getHeadWord(subject.getWordList(), this.sentenceSemGraph);
                tempProp.add(new AnnotatedPhrase(subject.getWordList(), subjRoot));
                tempProp.add(new AnnotatedPhrase(this.rel.getWordList().clone(), this.rel.getHeadWord()));
                tempProp.add(new AnnotatedPhrase(this.obj.getWordList().clone(), objRoot));
                AnnotatedProposition aProp = new AnnotatedProposition(tempProp.clone(), new Attribution());
                aProp.setExtractionType(EXTRACTION_TYPE.IMPL_ORG_NP_PERSON);
                this.propositions.add(aProp);
                tempProp.clear();
            }
            
            // Clean the variables
            this.subj.clear();
            this.obj.clear();
            this.rel.clear();
        }
    }
    
    /** If (NP+ PERSON) => "PERSON" "is" "NP" **/
    private void extractNounPerson() {
        // Reusable variables
        ObjectArrayList<AnnotatedPhrase> tempProp = new ObjectArrayList<>();
        IndexedWord subjRoot;
        IndexedWord objRoot;
        
        // Set the relation to be "is-a" relation
        this.setIsARelation();
        
        this.tPattern = TokenSequencePattern.compile(REGEX.T_NP_PERSON);
        this.tMatcher = this.tPattern.getMatcher(WordCollectionUtils.getCoreLabelList(this.sentence));
        while (this.tMatcher.find()){         
            for (IndexedWord w: WordCollectionUtils.toIndexedWordList(this.tMatcher.groupNodes())) {
                if (w.ner().equals(NE_TYPE.PERSON)) {
                    this.subj.addWordToList(w);
                }
                else {
                    if (isLemmaFemale(w.lemma())) {
                        this.obj.addWordToList(this.generateFemaleIndexedWord());
                    }
                    else if (isLemmaMale(w.lemma())) {
                        this.obj.addWordToList(this.generateMaleIndexedWord());
                    }
                    else if (Polarity.NEG_WORDS.contains(w.lemma().toLowerCase())) {
                        continue;
                    }
                    else {
                        this.obj.addWordToList(w);
                    }
                }
            }
                
            // Add the subj/rel/obj to the temporary proposition and then to the real propositions
            subjRoot = CoreNLPUtils.getHeadWord(this.subj.getWordList(), this.sentenceSemGraph);
            objRoot = CoreNLPUtils.getHeadWord(this.obj.getWordList(), this.sentenceSemGraph);
            tempProp.add(new AnnotatedPhrase(this.subj.getWordList().clone(), subjRoot));
            tempProp.add(new AnnotatedPhrase(this.rel.getWordList().clone(), this.rel.getHeadWord()));
            tempProp.add(new AnnotatedPhrase(this.obj.getWordList().clone(), objRoot));
            AnnotatedProposition aProp = new AnnotatedProposition(tempProp.clone(), new Attribution());
            aProp.setExtractionType(EXTRACTION_TYPE.IMPL_NP_PERSON);
            this.propositions.add(aProp);
                
            // Clean the variables
            tempProp.clear();
            this.subj.clear();
            this.obj.clear();
        }

        // Clear the relation
        this.rel.clear();
    }

    /**
     * @return an IndexedWord "male" for an implicit extraction
     */
    private IndexedWord generateMaleIndexedWord() {
        IndexedWord male = new IndexedWord();
        male.setWord("male");
        male.setOriginalText("male");
        male.setTag(POS_TAG.NN);
        male.setNER(NE_TYPE.NO_NER);
        male.setLemma("male");
        male.setValue("male");
        male.setIndex(-2);
        return male;
    }

    /**
     * @return an IndexedWord "female" for an implicit extraction
     */
    private IndexedWord generateFemaleIndexedWord() {
        IndexedWord female = new IndexedWord();
        female.setWord("female");
        female.setOriginalText("female");
        female.setTag(POS_TAG.NN);
        female.setNER(NE_TYPE.NO_NER);
        female.setLemma("female");
        female.setValue("female");
        female.setIndex(-2);
        return female;
    }

    private IndexedWord generatePrepositionOf() {
        IndexedWord ofWord = new IndexedWord();
        ofWord.setWord("of");
        ofWord.setOriginalText("of");
        ofWord.setTag(POS_TAG.IN);
        ofWord.setNER(NE_TYPE.NO_NER);
        ofWord.setLemma("of");
        ofWord.setValue("of");
        ofWord.setIndex(-2);
        return ofWord;
    }

    /**
     * Given a lemma of a word, checks if the lemma indicates "female"
     * @param lemma the lemma of a word
     * @return true if "female", false otherwise
     */
    private boolean isLemmaFemale(String lemma) {
        return lemma.toLowerCase().equals("mrs.") || lemma.toLowerCase().equals("ms.") ||
                lemma.toLowerCase().equals("mrs") || lemma.toLowerCase().equals("ms");
    }

    /**
     * Given a lemma of a word, checks if the lemma indicates "male"
     * @param lemma the lemma of a word
     * @return true if "male", false otherwise
     */
    private boolean isLemmaMale(String lemma) {
        return lemma.toLowerCase().equals("mr.") || lemma.toLowerCase().equals("mr");
    }
    
    /** "PERSON+ among (other) NP" => "PERSON" "is" "lemma(NP)" **/
    private void extractPersonAmongNP() {
        // Reusable variables
        IndexedWord tempWord;
        IndexedWord subjRoot;
        IndexedWord objRoot;
        ObjectArrayList<AnnotatedPhrase> tempProp = new ObjectArrayList<>();
        
        // Set the relation to be "is-a" relation
        this.setIsARelation();
        
        // Token regex patterns
        this.tPattern = TokenSequencePattern.compile(REGEX.T_PERSON_AMONG_NP);
        this.tMatcher = this.tPattern.getMatcher(WordCollectionUtils.getCoreLabelList(this.sentence));
        while (this.tMatcher.find()){       
            for (IndexedWord w: WordCollectionUtils.toIndexedWordList(this.tMatcher.groupNodes())) {
                if (w.ner().equals(NE_TYPE.PERSON))
                    this.subj.addWordToList(w);
                else {
                    if (!w.lemma().equals("among") && !w.lemma().equals("other")) {
                        tempWord = new IndexedWord(w);
                        tempWord.setWord(w.lemma());
                        this.obj.addWordToList(tempWord);
                    }
                }
            }
                
            // Add the subj/rel/obj to the temporary proposition and then to the real propositions
            subjRoot = CoreNLPUtils.getHeadWord(subj.getWordList(), this.sentenceSemGraph);
            objRoot = CoreNLPUtils.getHeadWord(obj.getWordList(), this.sentenceSemGraph);
            tempProp.add(new AnnotatedPhrase(this.subj.getWordList().clone(), subjRoot));
            tempProp.add(new AnnotatedPhrase(this.rel.getWordList().clone(), this.rel.getHeadWord()));
            tempProp.add(new AnnotatedPhrase(this.obj.getWordList().clone(), objRoot));
            AnnotatedProposition aProp = new AnnotatedProposition(tempProp.clone(), new Attribution());
            aProp.setExtractionType(EXTRACTION_TYPE.IMPL_PERSON_AMONG_NP);
            this.propositions.add(aProp);
                    
            // Clean the variables
            tempProp.clear();
            this.subj.clear();
            this.obj.clear();
        }
        
        // Clear the relation
        this.rel.clear(); 
    }
    
    /** Hearst pattern 1: NP_1 such as NP_2, NP_3, ... [and|or]? NP_n => "NP_2" "is" "NP_1", ... "NP_n" "is" "NP_1" **/
    private void extractHearst1() {
        // Reusable variables
        IndexedWord tempWord;
        IndexedWord subjRoot;
        IndexedWord objRoot;
        ObjectArrayList<AnnotatedPhrase> tempProp = new ObjectArrayList<>();
        
        // Set the relation to be "is-a" relation
        this.setIsARelation();
        
        // Pattern regex/matcher
        this.tPattern = TokenSequencePattern.compile(REGEX.T_HEARST_1);
        this.tMatcher = this.tPattern.getMatcher(WordCollectionUtils.getCoreLabelList(this.sentence));
        while (this.tMatcher.find()){    
            ObjectArrayList<IndexedWord> matchedWords = WordCollectionUtils.toIndexedWordList(this.tMatcher.groupNodes());
            int objInd = -1;
            
            // Define the object
            for (int i = 0; i < matchedWords.size(); i++){
                if (!matchedWords.get(i).lemma().equals("such")){
                    tempWord = new IndexedWord(matchedWords.get(i));
                    tempWord.setWord(matchedWords.get(i).lemma());
                    this.obj.addWordToList(tempWord);
                    objInd = i + 3;
                } else { 
                    break;
                }
            }
            
            // Define the subject(s)
            IndexedWord w;
            if (objInd > -1){
                for (int i = objInd; i < matchedWords.size(); i++){
                    w = matchedWords.get(i);
                    if ((w.lemma().equals(CHARACTER.COMMA) || w.lemma().equals("and") || w.lemma().equals("or")) && 
                            w.ner().equals(NE_TYPE.NO_NER)) {
                        // Add the subj/rel/obj to the temporary proposition and then to the real propositions
                        subjRoot = CoreNLPUtils.getHeadWord(this.subj.getWordList(), this.sentenceSemGraph);
                        objRoot = CoreNLPUtils.getHeadWord(this.obj.getWordList(), this.sentenceSemGraph);
                        tempProp.add(new AnnotatedPhrase(this.subj.getWordList().clone(), subjRoot));
                        tempProp.add(new AnnotatedPhrase(this.rel.getWordList().clone(), this.rel.getHeadWord()));
                        tempProp.add(new AnnotatedPhrase(this.obj.getWordList().clone(), objRoot));
                        AnnotatedProposition aProp = new AnnotatedProposition(tempProp.clone(), new Attribution());
                        aProp.setExtractionType(EXTRACTION_TYPE.IMPL_HEARST_NP_SUCH_AS_NP);
                        this.propositions.add(aProp);
                            
                        // Clean the variables
                        tempProp.clear();
                        this.subj.clear();
                    } else {
                        this.subj.addWordToList(w);
                    }
                }
            }
            
            // Add the subj/rel/obj to the temporary proposition and then to the real propositions
            subjRoot = CoreNLPUtils.getHeadWord(this.subj.getWordList(), this.sentenceSemGraph);
            objRoot = CoreNLPUtils.getHeadWord(this.obj.getWordList(), this.sentenceSemGraph);

            // Check if this is eligible for extraction
            if (!argsNERCheckForBeRel(subjRoot, objRoot)) {
                // Clean the variables
                tempProp.clear();
                this.subj.clear();
                this.obj.clear();
                this.rel.clear();
                return;
            }

            tempProp.add(new AnnotatedPhrase(this.subj.getWordList().clone(), subjRoot));
            tempProp.add(new AnnotatedPhrase(this.rel.getWordList().clone(), this.rel.getHeadWord()));
            tempProp.add(new AnnotatedPhrase(this.obj.getWordList().clone(), objRoot));
            AnnotatedProposition aProp = new AnnotatedProposition(tempProp.clone(), new Attribution());
            aProp.setExtractionType(EXTRACTION_TYPE.IMPL_HEARST_NP_SUCH_AS_NP);
            this.propositions.add(aProp);
                    
            // Clean the variables
            tempProp.clear();
            this.subj.clear();
            this.obj.clear();
        }    
        
        // Clear the relation
        this.rel.clear();
    }
    
    /** Hearst pattern 2: if "NP_1 like NP_2, NP_3, ... [and|or] NP_n => "NP_2" "is" NP_1", ... "NP_n" "is" "NP_1"  **/
    private void extractHearst2() {
        // Reusable variables
        IndexedWord tempWord;
        IndexedWord subjRoot;
        IndexedWord objRoot;
        ObjectArrayList<AnnotatedPhrase> tempProp = new ObjectArrayList<>();
        
        // Set the relation to be "is-a" relation
        this.setIsARelation();
        
        this.tPattern = TokenSequencePattern.compile(REGEX.T_HEARST_2);
        this.tMatcher = this.tPattern.getMatcher(WordCollectionUtils.getCoreLabelList(this.sentence));
        while (this.tMatcher.find()){    
            ObjectArrayList<IndexedWord> matchedWords = WordCollectionUtils.toIndexedWordList(this.tMatcher.groupNodes());
            int objInd = -1;
            
            // Define the object
            for (int i = 0; i < matchedWords.size(); i++){
                if (!matchedWords.get(i).lemma().equals("like")){
                    tempWord = new IndexedWord(matchedWords.get(i));
                    tempWord.setWord(matchedWords.get(i).lemma());
                    this.obj.addWordToList(tempWord);
                    objInd = i + 2;
                } else { 
                    break;
                }
            }
            
            // Define the subject(s)
            IndexedWord w;
            if (objInd > -1){
                for (int i = objInd; i < matchedWords.size(); i++){
                    w = matchedWords.get(i);
                    if ((w.lemma().equals(CHARACTER.COMMA) || w.lemma().equals("and") || w.lemma().equals("or")) && 
                            w.ner().equals(NE_TYPE.NO_NER)) {
                        // Add the subj/rel/obj to the temporary proposition and then to the real propositions
                        subjRoot = CoreNLPUtils.getHeadWord(this.subj.getWordList(), this.sentenceSemGraph);
                        objRoot = CoreNLPUtils.getHeadWord(this.obj.getWordList(), this.sentenceSemGraph);
                        tempProp.add(new AnnotatedPhrase(this.subj.getWordList().clone(), subjRoot));
                        tempProp.add(new AnnotatedPhrase(this.rel.getWordList().clone(), this.rel.getHeadWord()));
                        tempProp.add(new AnnotatedPhrase(this.obj.getWordList().clone(), objRoot));
                        AnnotatedProposition aProp = new AnnotatedProposition(tempProp.clone(), new Attribution());
                        aProp.setExtractionType(EXTRACTION_TYPE.IMPL_HEARST_NP_LIKE_NP);
                        this.propositions.add(aProp);
                            
                        // Clean the variables
                        tempProp.clear();
                        this.subj.clear();
                    } else {
                        this.subj.addWordToList(w);
                    }
                }
            }
            
            // Add the subj/rel/obj to the temporary proposition and then to the real propositions
            subjRoot = CoreNLPUtils.getHeadWord(this.subj.getWordList(), this.sentenceSemGraph);
            objRoot = CoreNLPUtils.getHeadWord(this.obj.getWordList(), this.sentenceSemGraph);

            // Check if this is eligible for extraction
            if (!argsNERCheckForBeRel(subjRoot, objRoot)) {
                // Clean the variables
                tempProp.clear();
                this.subj.clear();
                this.obj.clear();
                this.rel.clear();
                return;
            }

            tempProp.add(new AnnotatedPhrase(this.subj.getWordList().clone(), subjRoot));
            tempProp.add(new AnnotatedPhrase(this.rel.getWordList().clone(), this.rel.getHeadWord()));
            tempProp.add(new AnnotatedPhrase(this.obj.getWordList().clone(), objRoot));
            AnnotatedProposition aProp = new AnnotatedProposition(tempProp.clone(), new Attribution());
            aProp.setExtractionType(EXTRACTION_TYPE.IMPL_HEARST_NP_LIKE_NP);
            this.propositions.add(aProp);
                    
            // Clean the variables
            tempProp.clear();
            this.subj.clear();
            this.obj.clear();
        }
        
        // Clear the relation
        this.rel.clear();
    }
    
    /** Hearst pattern 2_2: such NP_1 as NP_1, NP_2, ... [and|or] NP_n => "NP_2" "is" "NP_1", ... "NP_n", "is", "NP_1" **/
    private void extractHearst2_2() {
        // Reusable variables
        IndexedWord tempWord;
        IndexedWord subjRoot;
        IndexedWord objRoot;
        ObjectArrayList<AnnotatedPhrase> tempProp = new ObjectArrayList<>();
        
        // Set the relation to be "is-a" relation
        this.setIsARelation();
        
        this.tPattern = TokenSequencePattern.compile(REGEX.T_HEARST_2_2);
        this.tMatcher = this.tPattern.getMatcher(WordCollectionUtils.getCoreLabelList(this.sentence));
        while (this.tMatcher.find()){    
            ObjectArrayList<IndexedWord> mWords = WordCollectionUtils.toIndexedWordList(this.tMatcher.groupNodes());
            int objInd = -1;
            
            // Define the object
            for (int i = 1; i < mWords.size(); i++) {        
                if (!mWords.get(i).lemma().equals("as")) {
                    tempWord = mWords.get(i);
                    tempWord.setWord(mWords.get(i).lemma());
                    this.obj.addWordToList(tempWord);
                    objInd = i + 2;
                } else break;
            }
            
            // Define subject(s) and add them to the proposition list
            for (int i = objInd; i < mWords.size(); i++) {
                tempWord = mWords.get(i);
                if ((tempWord.lemma().equals(CHARACTER.COMMA) || tempWord.lemma().equals("and") || 
                        tempWord.lemma().equals("or")) && 
                        tempWord.ner().equals(NE_TYPE.NO_NER)){
                    // Add the subj/rel/obj to the temporary proposition and then to the real propositions
                    subjRoot = CoreNLPUtils.getHeadWord(this.subj.getWordList(), this.sentenceSemGraph);
                    objRoot = CoreNLPUtils.getHeadWord(this.obj.getWordList(), this.sentenceSemGraph);
                    tempProp.add(new AnnotatedPhrase(this.subj.getWordList().clone(), subjRoot));
                    tempProp.add(new AnnotatedPhrase(this.rel.getWordList().clone(), this.rel.getHeadWord()));
                    tempProp.add(new AnnotatedPhrase(this.obj.getWordList().clone(), objRoot));
                    AnnotatedProposition aProp = new AnnotatedProposition(tempProp.clone(), new Attribution());
                    aProp.setExtractionType(EXTRACTION_TYPE.IMPL_HEARST_SUCH_NP_AS_NP);
                    this.propositions.add(aProp);
                        
                    // Clean the variables
                    tempProp.clear();
                    this.subj.clear();
                } else {
                    this.subj.addWordToList(tempWord);
                }
            }
            
            // Add the subj/rel/obj to the temporary proposition and then to the real propositions
            subjRoot = CoreNLPUtils.getHeadWord(this.subj.getWordList(), this.sentenceSemGraph);
            objRoot = CoreNLPUtils.getHeadWord(this.obj.getWordList(), this.sentenceSemGraph);

            // Check if this is eligible for extraction
            if (!argsNERCheckForBeRel(subjRoot, objRoot)) {
                // Clean the variables
                tempProp.clear();
                this.subj.clear();
                this.obj.clear();
                this.rel.clear();
                return;
            }

            tempProp.add(new AnnotatedPhrase(this.subj.getWordList().clone(), subjRoot));
            tempProp.add(new AnnotatedPhrase(this.rel.getWordList().clone(), this.rel.getHeadWord()));
            tempProp.add(new AnnotatedPhrase(this.obj.getWordList().clone(), objRoot));
            AnnotatedProposition aProp = new AnnotatedProposition(tempProp.clone(), new Attribution());
            aProp.setExtractionType(EXTRACTION_TYPE.IMPL_HEARST_SUCH_NP_AS_NP);
            this.propositions.add(aProp);
                    
            // Clean the variables
            tempProp.clear();
            this.subj.clear();
            this.obj.clear();
        }
        
        // Clear the relation
        this.rel.clear();
    }
    
    /** Hearst pattern NP_1, NP_2, ... [,|and|or] other NP_n **/
    private void extractHearst3() {
        // Reusable variables
        IndexedWord tempWord;
        IndexedWord subjRoot;
        IndexedWord objRoot;
        ObjectArrayList<AnnotatedPhrase> tempProp = new ObjectArrayList<>();
        
        // Set the relation to be "is-a" relation
        this.setIsARelation();
        
        this.tPattern = TokenSequencePattern.compile(REGEX.T_HEARST_3);
        this.tMatcher = this.tPattern.getMatcher(WordCollectionUtils.getCoreLabelList(this.sentence));
        while (this.tMatcher.find()){    
            ObjectArrayList<IndexedWord> mWords = WordCollectionUtils.toIndexedWordList(this.tMatcher.groupNodes());
            
            // Create the object
            boolean flag = false;
            for (IndexedWord w: mWords) {
                if (flag) {
                    tempWord = new IndexedWord(w);
                    tempWord.setWord(w.lemma());
                    this.obj.addWordToList(tempWord);
                }
                if (w.lemma().equals("other")){
                    flag = true;
                }
            }
            
            // Create the subject(s) and add the extractions to the propositions' list
            for (IndexedWord w: mWords) {
                if (w.lemma().equals("other"))
                    break;
                if ((w.lemma().equals(CHARACTER.COMMA) || w.lemma().equals("and") || w.lemma().equals("or")) && 
                        w.ner().equals(NE_TYPE.NO_NER)) {
                    // Add the subj/rel/obj to the temporary proposition and then to the real propositions
                    subjRoot = CoreNLPUtils.getHeadWord(this.subj.getWordList(), this.sentenceSemGraph);
                    objRoot = CoreNLPUtils.getHeadWord(this.obj.getWordList(), this.sentenceSemGraph);
                    tempProp.add(new AnnotatedPhrase(this.subj.getWordList().clone(), subjRoot));
                    tempProp.add(new AnnotatedPhrase(this.rel.getWordList().clone(), this.rel.getHeadWord()));
                    tempProp.add(new AnnotatedPhrase(this.obj.getWordList().clone(), objRoot));
                    AnnotatedProposition aProp = new AnnotatedProposition(tempProp.clone(), new Attribution());
                    aProp.setExtractionType(EXTRACTION_TYPE.IMPL_HEARST_NP_AND_OR_OTHER_NP);
                    this.propositions.add(aProp);
                        
                    // Clean the variables
                    tempProp.clear();
                    this.subj.clear();
                } else {
                    this.subj.addWordToList(w);
                }
            }
            
            // Add the subj/rel/obj to the temporary proposition and then to the real propositions
            subjRoot = CoreNLPUtils.getHeadWord(this.subj.getWordList(), this.sentenceSemGraph);
            objRoot = CoreNLPUtils.getHeadWord(this.obj.getWordList(), this.sentenceSemGraph);

            if (subjRoot == null || objRoot == null || !argsNERCheckForBeRel(subjRoot, objRoot)) {
                // Clean the variables
                tempProp.clear();
                this.subj.clear();
                this.obj.clear();
                continue;
            }

            tempProp.add(new AnnotatedPhrase(this.subj.getWordList().clone(), subjRoot));
            tempProp.add(new AnnotatedPhrase(this.rel.getWordList().clone(), this.rel.getHeadWord()));
            tempProp.add(new AnnotatedPhrase(this.obj.getWordList().clone(), objRoot));
            AnnotatedProposition aProp = new AnnotatedProposition(tempProp.clone(), new Attribution());
            aProp.setExtractionType(EXTRACTION_TYPE.IMPL_HEARST_NP_AND_OR_OTHER_NP);
            this.propositions.add(aProp);
                    
            // Clean the variables
            tempProp.clear();
            this.subj.clear();
            this.obj.clear();
        }
        
        // Clear the relation
        this.rel.clear();
    }
    
    /**  NP , including (NP ,)* [or|and] NP  **/
    private void extractHearst4() {
        // Reusable variables
        IndexedWord tempWord;
        IndexedWord subjRoot;
        IndexedWord objRoot;
        ObjectArrayList<AnnotatedPhrase> tempProp = new ObjectArrayList<>();
        
        // Set the relation to be "is-a" relation
        this.setIsARelation();
        
        this.tPattern = TokenSequencePattern.compile(REGEX.T_HEARST_4);
        this.tMatcher = this.tPattern.getMatcher(WordCollectionUtils.getCoreLabelList(this.sentence));
        while (this.tMatcher.find()){    
            ObjectArrayList<IndexedWord> mWords = WordCollectionUtils.toIndexedWordList(this.tMatcher.groupNodes());

            // Detect object
            int objInd = -1;
            for (int i = 0; i < mWords.size(); i++) {
                if (mWords.get(i).lemma().equals(CHARACTER.COMMA) || mWords.get(i).word().equals("including") ||
                        mWords.get(i).word().equals("especially")){
                    objInd = i + 2;
                    break;
                }
                this.obj.addWordToList(mWords.get(i));
            }
        
            // Create subject(s) and add to propositions
            for (int i = objInd; i < mWords.size(); i++) {
                tempWord = mWords.get(i);
                if ((tempWord.lemma().equals(CHARACTER.COMMA) || tempWord.lemma().equals("and") || 
                        tempWord.lemma().equals("or")) && tempWord.ner().equals(NE_TYPE.NO_NER)) {
                    // Add the subj/rel/obj to the temporary proposition and then to the real propositions
                    subjRoot = CoreNLPUtils.getHeadWord(this.subj.getWordList(), this.sentenceSemGraph);
                    objRoot = CoreNLPUtils.getHeadWord(this.obj.getWordList(), this.sentenceSemGraph);
                    tempProp.add(new AnnotatedPhrase(this.subj.getWordList().clone(), subjRoot));
                    tempProp.add(new AnnotatedPhrase(this.rel.getWordList().clone(), this.rel.getHeadWord()));
                    tempProp.add(new AnnotatedPhrase(this.obj.getWordList().clone(), objRoot));
                    AnnotatedProposition aProp = new AnnotatedProposition(tempProp.clone(), new Attribution());
                    aProp.setExtractionType(EXTRACTION_TYPE.IMPL_HEARST_NP_INCLUDING_NP);
                    this.propositions.add(aProp);
                    
                    // Clean the variables
                    tempProp.clear();
                    this.subj.clear();
                } else {
                    this.subj.addWordToList(tempWord);
                }
            }
        
            // Add the subj/rel/obj to the temporary proposition and then to the real propositions
            subjRoot = CoreNLPUtils.getHeadWord(this.subj.getWordList(), this.sentenceSemGraph);
            objRoot = CoreNLPUtils.getHeadWord(this.obj.getWordList(), this.sentenceSemGraph);

            // Check if this is eligible extraction
            if (!argsNERCheckForBeRel(subjRoot, objRoot)) {
                // Clean the variables
                tempProp.clear();
                this.subj.clear();
                this.obj.clear();
                continue;
            }

            tempProp.add(new AnnotatedPhrase(this.subj.getWordList().clone(), subjRoot));
            tempProp.add(new AnnotatedPhrase(this.rel.getWordList().clone(), this.rel.getHeadWord()));
            tempProp.add(new AnnotatedPhrase(this.obj.getWordList().clone(), objRoot));
            AnnotatedProposition aProp = new AnnotatedProposition(tempProp.clone(), new Attribution());
            aProp.setExtractionType(EXTRACTION_TYPE.IMPL_HEARST_NP_INCLUDING_NP);
            this.propositions.add(aProp);
                
            // Clean the variables
            tempProp.clear();
            this.subj.clear();
            this.obj.clear();
        }
        
        // Clear the relation
        this.rel.clear();
    }
    
    /** If "city|town of LOCATION" => "LOCATION" "is" "city|town" **/
    private void extractCityOfLocation() {
        // Reusable variable
        ObjectArrayList<AnnotatedPhrase> tempProp = new ObjectArrayList<>();
        IndexedWord subjRoot;
        IndexedWord objRoot;
        
        // Set the relation to be "is-a" relation
        this.setIsARelation();
        
        this.tPattern = TokenSequencePattern.compile(REGEX.T_CITY_OF_LOC);
        this.tMatcher = tPattern.getMatcher(WordCollectionUtils.getCoreLabelList(this.sentence));
        while (this.tMatcher.find()){    
            ObjectArrayList<IndexedWord> mWords = WordCollectionUtils.toIndexedWordList(this.tMatcher.groupNodes());
            for (IndexedWord w: mWords) {
                if (!w.ner().equals(NE_TYPE.LOCATION) && !w.tag().equals(POS_TAG.IN))
                    this.obj.addWordToList(w);
                else{ 
                    if (!w.tag().equals(POS_TAG.IN))
                        this.subj.addWordToList(w);
                }
            }
            
            // Add the subj/rel/obj to the temporary proposition and then to the real propositions
            subjRoot = CoreNLPUtils.getHeadWord(this.subj.getWordList(), this.sentenceSemGraph);
            objRoot = CoreNLPUtils.getHeadWord(this.obj.getWordList(), this.sentenceSemGraph);
            tempProp.add(new AnnotatedPhrase(this.subj.getWordList().clone(), subjRoot));
            tempProp.add(new AnnotatedPhrase(this.rel.getWordList().clone(), this.rel.getHeadWord()));
            tempProp.add(new AnnotatedPhrase(this.obj.getWordList().clone(), objRoot));
            AnnotatedProposition aProp = new AnnotatedProposition(tempProp.clone(), new Attribution());
            aProp.setExtractionType(EXTRACTION_TYPE.IMPL_CITY_OF_LOC);
            this.propositions.add(aProp);
                    
            // Clean the variables
            tempProp.clear();
            this.subj.clear();
            this.obj.clear();
        }
        
        // Clear the relation
        this.rel.clear();
    }
    
    public ObjectArrayList<AnnotatedProposition> getImplicitExtractions() {
        return this.propositions;
    }
    public ObjectArrayList<AnnotatedProposition> getPropositions() {
        return this.propositions;
    }
}
