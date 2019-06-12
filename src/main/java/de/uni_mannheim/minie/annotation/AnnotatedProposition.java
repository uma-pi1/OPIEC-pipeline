package de.uni_mannheim.minie.annotation;

import de.uni_mannheim.clausie.clause.Clause;
import de.uni_mannheim.clausie.constituent.Constituent;
import de.uni_mannheim.clausie.phrase.Phrase;
import de.uni_mannheim.clausie.proposition.Proposition;
import de.uni_mannheim.constant.*;
import de.uni_mannheim.minie.annotation.SpaTe.SpaTeCore;
import de.uni_mannheim.minie.annotation.SpaTe.space.PropSpace;
import de.uni_mannheim.minie.annotation.SpaTe.time.Time;
import de.uni_mannheim.minie.annotation.factuality.Factuality;
import de.uni_mannheim.minie.annotation.factuality.Modality;
import de.uni_mannheim.minie.annotation.factuality.Polarity;
import de.uni_mannheim.minie.annotation.SpaTe.space.Space;
import de.uni_mannheim.minie.annotation.SpaTe.space.SpaceUtils;
import de.uni_mannheim.minie.annotation.SpaTe.time.PropTime;
import de.uni_mannheim.minie.annotation.SpaTe.time.TimeUtils;

import de.uni_mannheim.utils.coreNLP.WordCollectionUtils;
import de.uni_mannheim.utils.coreNLP.WordUtils;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.ling.tokensregex.TokenSequenceMatcher;
import edu.stanford.nlp.ling.tokensregex.TokenSequencePattern;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import edu.stanford.nlp.util.CoreMap;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.*;

/**
 * @author Kiril Gashteovski
 */
public class AnnotatedProposition {
    /** The annotated proposition is a triple, a list of annotated phrases **/
    private ObjectArrayList<AnnotatedPhrase> triple;
    /** The attribution of the triple if found any **/
    private Attribution attribution;
    /** Factuality of the triple **/
    private Factuality factuality;
    /** The ID is w.r.t the sentence from which the proposition is extracted. The default ID is -1 **/
    private int id;
    /** The temporal annotation of the triple **/
    private PropTime time;
    /** The spatial annotation of the triple **/
    private PropSpace space;
    /** All the words from the triple as a list **/
    private ObjectArrayList<IndexedWord> allwords;
    /** All the words from the triples separated as a list of constituents (n-ary tuple) **/
    private ObjectArrayList<Phrase> nAryConstituents;
    /** Constituent types for the nAryConstituents **/
    private ObjectArrayList<Constituent.Type> nAryConstituentTypes;
    /** Extraction type **/
    private String extractionType;

    // The clause from which it was extracted (Used for confidence)
    private Clause clause;
    public Clause getClause() {
        return clause;
    }
    public void setClause(Clause clause) {
        this.clause = clause;
    }

    // store the confidence score
    private double confidence;
    public double getConfidence() {
        return confidence;
    }
    public void setConfidence(double confidence) {
        this.confidence = confidence;
    }


    /** Default constructor: empty triple, default attribution, polarity, modality **/
    public AnnotatedProposition(){
        this.triple = new ObjectArrayList<>();
        this.attribution = new Attribution();
        this.factuality = new Factuality();
        this.id = -1; // the default ID
        this.time = new PropTime();
        this.space = new PropSpace();
        this.allwords = new ObjectArrayList<>();
        this.nAryConstituents = new ObjectArrayList<>();
        this.nAryConstituentTypes = new ObjectArrayList<>();
        this.extractionType = "";
    }

    /** Copy constructor **/
    public AnnotatedProposition(AnnotatedProposition p){
        this.triple = p.getTriple().clone();
        this.attribution = p.getAttribution();
        this.factuality = new Factuality();
        this.id = p.getId();
        this.time = p.getTime();
        this.space = new PropSpace();
        this.allwords = p.getAllwords();
        this.nAryConstituents = p.getnAryConstituents();
        this.nAryConstituentTypes = p.getnAryConstituentTypes();
        this.confidence = p.getConfidence();
        this.extractionType = p.getExtractionType();
    }

    /** Constructor given list of phrases only **/
    public AnnotatedProposition(ObjectArrayList<AnnotatedPhrase> t){
        this.triple = t;
        this.attribution = new Attribution();
        this.factuality = new Factuality();
        this.id = -1;
        this.time = new PropTime();
        this.space = new PropSpace();
        this.allwords = new ObjectArrayList<>();
        for (AnnotatedPhrase ap: t) {
            this.allwords.addAll(ap.getWordList());
        }
        this.nAryConstituents = new ObjectArrayList<>();
        this.nAryConstituentTypes = new ObjectArrayList<>();
    }

    /** Constructor given list of phrases and attribution only **/
    public AnnotatedProposition(ObjectArrayList<AnnotatedPhrase> t, Attribution s){
        this.triple = t;
        this.attribution = s;
        this.factuality = new Factuality();
        this.id = -1;
        this.time = new PropTime();
        this.space = new PropSpace();
        this.allwords = new ObjectArrayList<>();
        for (AnnotatedPhrase ap: t) {
            this.allwords.addAll(ap.getWordList());
        }
        this.nAryConstituents = new ObjectArrayList<>();
        this.nAryConstituentTypes = new ObjectArrayList<>();
        this.extractionType = "";
    }

    /** Constructor given list of phrases and id only **/
    public AnnotatedProposition(ObjectArrayList<AnnotatedPhrase> t, int id){
        this.triple = t;
        this.attribution = new Attribution();
        this.factuality = new Factuality();
        this.id = id;
        this.time = new PropTime();
        this.space = new PropSpace();
        this.allwords = new ObjectArrayList<>();
        for (AnnotatedPhrase ap: t) {
            this.allwords.addAll(ap.getWordList());
        }
        this.nAryConstituents = new ObjectArrayList<>();
        this.nAryConstituentTypes = new ObjectArrayList<>();
        this.extractionType = "";
    }

    /** Constructor given list of phrases, attribution and ID **/
    public AnnotatedProposition(ObjectArrayList<AnnotatedPhrase> t, Attribution s, int id){
        this.triple = t;
        this.attribution = s;
        this.factuality = new Factuality();
        this.id = id;
        this.time = new PropTime();
        this.space = new PropSpace();
        this.allwords = new ObjectArrayList<>();
        for (AnnotatedPhrase ap: t) {
            this.allwords.addAll(ap.getWordList());
        }
        this.nAryConstituents = new ObjectArrayList<>();
        this.nAryConstituentTypes = new ObjectArrayList<>();
        this.extractionType = "";
    }

    // Setters
    public void setTriple(ObjectArrayList<AnnotatedPhrase> t){
        this.triple = t.clone();
    }
    public void setAttribution(Attribution s){
        this.attribution = s;
    }
    public void setPolarity(Polarity p){
        this.factuality.setPolarity(p);
    }
    public void setModality(Modality m){
        this.factuality.setModality(m);
    }
    public void setSubject(AnnotatedPhrase subj){
        this.triple.set(0, subj);
    }
    public void setRelation(AnnotatedPhrase rel){
        this.triple.set(1, rel);
    }
    public void setObject(AnnotatedPhrase obj){
        this.triple.set(2, obj);
    }
    public void setId(int id){
        this.id = id;
    }
    public void setTime(PropTime time) {
        this.time = time;
    }
    public void setnAryConstituents(ObjectArrayList<Phrase> p) {
        this.nAryConstituents = p.clone();
    }
    public void setnAryConstituentTypes(ObjectArrayList<Constituent.Type> types) {
        this.nAryConstituentTypes = types.clone();
    }
    public void setExtractionType(String type) {
        this.extractionType = type;
    }

    // Getters
    public ObjectArrayList<AnnotatedPhrase> getTriple(){
        return this.triple;
    }
    public AnnotatedPhrase getSubject(){
        return this.triple.get(0);
    }
    public AnnotatedPhrase getRelation(){
        return this.triple.get(1);
    }
    /** If the proposition is consisted of 2 constituents, return empty phrase **/
    public AnnotatedPhrase getObject(){
        if (this.triple.size() == 3)
            return this.triple.get(2);
        else
            return new AnnotatedPhrase();
    }
    public ObjectArrayList<Phrase> getnAryConstituents() {
        return this.nAryConstituents;
    }
    public Attribution getAttribution(){
        return this.attribution;
    }
    public Polarity getPolarity(){
        return this.factuality.getPolarity();
    }
    public Modality getModality(){
        return this.factuality.getModality();
    }
    public int getId(){
        return this.id;
    }
    public PropTime getTime() {
        return this.time;
    }
    public PropSpace getSpace() {
        return this.space;
    }
    public ObjectArrayList<IndexedWord> getAllwords() {
        return this.allwords;
    }
    public String getExtractionType() {
        return this.extractionType;
    }

    public ObjectArrayList<Constituent.Type> getnAryConstituentTypes() {
        return this.nAryConstituentTypes;
    }

    /**
     * Get the quantities from the i-th phrase
     * @param i: the index of the annotated phrase
     * @return list of quantities for the phrase
     */
    public ObjectArrayList<Quantity> getQuantities(int i){
        return this.getTriple().get(i).getQuantities();
    }

    /**
     * @return all the quantities in the annotated proposition
     */
    public ObjectArrayList<Quantity> getAllQuantities() {
        ObjectArrayList<Quantity> quantities = new ObjectArrayList<>();
        for (int i = 0; i < this.getTriple().size(); i++) {
            quantities.addAll(this.getTriple().get(i).getQuantities());
        }
        return quantities;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();

        // Write the triple
        sb.append(this.getTripleAsString());

        // Write the attribution (if found any)
        if (this.getAttribution().getAttributionPhrase() != null){
            sb.append("\t Source:");
            sb.append(this.getAttribution());
        }

        // Write polarity (if negative)
        if (this.getPolarity().getType() == Polarity.Type.NEGATIVE){
            sb.append("\t Polarity: " );
            sb.append(Polarity.ST_NEGATIVE);
        }

        // Write modality (if poss found)
        if (this.getModality().getModalityType() == Modality.Type.POSSIBILITY){
            sb.append("\t Modality:" );
            sb.append(Modality.ST_POSSIBILITY);
        }

        // Write quantities if there are any
        for (int j = 0; j < this.getTriple().size(); j++){
            if (this.getQuantities(j).size() > 0){
                sb.append("\t Quantifiers:");
                for (Quantity q: this.getQuantities(j)){
                    sb.append(q.toString());
                }
            }
        }

        // Add time/space to the string
        sb.append(this.timeToString());
        sb.append(this.spaceToString());

        return sb.toString();
    }

    /** RelTime to string **/
    private String timeToString() {
        StringBuilder sb = new StringBuilder();

        if (!this.time.isEmpty()) {
            sb.append("\tTime: (");
            if (this.time.size() == 1) {
                for (Time t : this.time.getTimes()) {
                    if (t.hasPredicate()) {
                        sb.append("pred=");
                        sb.append(t.getPredicate().word());
                        sb.append(SEPARATOR.COMMA);
                        sb.append(SEPARATOR.SPACE);
                    }
                    sb.append(t.temporalWordsToString());
                }
            } else {
                String prefix = "";
                for (Time t : this.time.getTimes()) {
                    sb.append(prefix);
                    prefix = "; ";
                    if (t.hasPredicate()) {
                        sb.append("pred=");
                        sb.append(t.getPredicate().word());
                        sb.append(SEPARATOR.COMMA);
                        sb.append(SEPARATOR.SPACE);
                    }
                    sb.append(t.temporalWordsToString());
                }
            }
            sb.append(")");
        }

        for (AnnotatedPhrase ap: this.triple) {
            if (ap.hasTempEx()) {
                sb.append("\tTime:[");
                sb.append( ap.getTime().temporalWordsToString());
                sb.append(",");
                sb.append(ap.getTemporallyModifiedWord().word());
                sb.append("]");
            }
        }

        return sb.toString();
    }

    private String spaceToString() {
        StringBuilder sb = new StringBuilder();

        if (!this.space.isEmpty()) {
            sb.append("\tSpace: (");
            if (this.space.size() == 1) {
                for (Space s : this.space.getSpaces()) {
                    if (s.hasPredicate()) {
                        sb.append("pred=");
                        sb.append(s.getPredicate().word());
                        sb.append(SEPARATOR.COMMA);
                        sb.append(SEPARATOR.SPACE);
                    }
                    sb.append(s.spatialWordsToString());
                }
            } else {
                String prefix = "";
                for (Space s : this.space.getSpaces()) {
                    sb.append(prefix);
                    prefix = "; ";
                    if (s.hasPredicate()) {
                        sb.append("pred=");
                        sb.append(s.getPredicate().word());
                        sb.append(SEPARATOR.COMMA);
                        sb.append(SEPARATOR.SPACE);
                    }
                    sb.append(s.spatialWordsToString());
                }
            }
            sb.append(")");
        }

        for (AnnotatedPhrase ap: this.triple) {
            if (ap.hasSpaceEx()) {
                sb.append("\tSpace:[");
                sb.append( ap.getSpace().spatialWordsToString());
                /*if (ap.getSpace().hasPredicate()) {
                    sb.append("pred=");
                    sb.append(ap.getSpace().getPredicate().word());
                    sb.append(SEPARATOR.SPACE);
                }*/
                sb.append(",");
                sb.append(ap.getSpatiallyModifiedWord().word());
                sb.append("]");
            }
        }

        return sb.toString();
    }

    /** Get the extraction as string **/
    public String getTripleAsString() {
        StringBuilder sb = new StringBuilder();

        // Write the triple
        for (int j = 0; j < this.getTriple().size(); j++){
            sb.append(CHARACTER.QUOTATION_MARK);
            sb.append(getTriple().get(j).getWords());
            sb.append(CHARACTER.QUOTATION_MARK);
            sb.append(SEPARATOR.TAB);
        }

        return sb.toString();
    }

    /** Get factuality as a string in format "(POLARITY, MODALITY)" **/
    public String getFactualityAsString() {
        StringBuilder sb = new StringBuilder();
        sb.append(CHARACTER.LPARENTHESIS);
        if (this.factuality.getPolarityType() == Polarity.Type.POSITIVE)
            sb.append(Polarity.ST_PLUS);
        else
            sb.append(Polarity.ST_MINUS);
        sb.append(CHARACTER.COMMA);
        if (this.factuality.getModalityType() == Modality.Type.CERTAINTY)
            sb.append(Modality.ST_CT);
        else
            sb.append(Modality.ST_PS);
        sb.append(CHARACTER.RPARENTHESIS);
        return sb.toString();
    }

    public String toStringAllAnnotations(){
        StringBuilder sb = new StringBuilder();

        // Write the triple
        sb.append(CHARACTER.LPARENTHESIS);
        IndexedWord tempWord;
        for (int i = 0; i < this.getTriple().size(); i++){
            for (int j = 0; j < this.getTriple().get(i).getWordList().size(); j++) {
                tempWord =  this.getTriple().get(i).getWordList().get(j);
                sb.append(tempWord.word());
                if (j == this.getTriple().get(i).getWordList().size() - 1) {
                    if (i < 2) {
                        sb.append("; ");
                    }
                } else {
                    sb.append(" ");
                }
            }
           /*sb.append("\"");
           sb.append(this.getTriple().get(j).getWords());
           sb.append("\"\t");*/
        }
        sb.append(")");

        // Write factuality
        sb.append("\tfactuality: (");
        if (this.getPolarity().getType() == Polarity.Type.NEGATIVE)
            sb.append("-,");
        else
            sb.append("+,");
        if (this.getModality().getModalityType() == Modality.Type.POSSIBILITY)
            sb.append("PS)");
        else
            sb.append("CT)");

        // Write quantities
        sb.append("\tquantities: (");
        ObjectArrayList<Quantity> quantities = new ObjectArrayList<>();
        for (int j = 0; j < this.getTriple().size(); j++){
            if (this.getQuantities(j).size() > 0){
                quantities.addAll(this.getQuantities(j));
            }
        }
        for (int i = 0; i < quantities.size(); i++) {
            sb.append(quantities.get(i).toString());
            if (i < quantities.size() - 1)
                sb.append(" ");
        }
        sb.append(")");

        // Write the attribution (if found any)
        sb.append("\tattribution: ");
        if (this.getAttribution().getAttributionPhrase() != null){
            sb.append(this.getAttribution().toString());
        } else {
            sb.append("NO SOURCE DETECTED");
        }

        // Add time/space to the string
        sb.append(this.timeToString());
        sb.append(this.spaceToString());

        return sb.toString();
    }

    /**
     * Get the proposition's words to a string (separated by a space) without the annotations.
     * Each element of the proposition is separated by quotation marks
     * E.g. "John""lives in""Canada"
     * @return triple's words as a string separated by a space
     */
    public String wordsToString(){
        StringBuilder sb = new StringBuilder();
        // Write the triple
        for (int i = 0; i < this.getTriple().size(); i++){
            for (int j = 0; j < this.getTriple().get(i).getWordList().size(); j++) {
                if (j == 0) {
                    sb.append(CHARACTER.QUOTATION_MARK);
                }
                sb.append(this.getTriple().get(i).getWordList().get(j).word());
                if (j == this.getTriple().get(i).getWordList().size()-1) {
                    sb.append(CHARACTER.QUOTATION_MARK);
                } else {
                    sb.append(SEPARATOR.SPACE);
                }
            }
        }
        return sb.toString().trim();
    }

    /**
     * Given a list of TempEx expressions (a list of list of words), and the dependency parse of the sentence, detect
     * the time of the annotated proposition (on triple's level)
     * @param tempAnnotations list of temporal annotations
     * @param sg dependency parse of the sentence
     */
    public void detectTime(ObjectArrayList<Time> tempAnnotations, SemanticGraph sg) {
        // Get the indices of the words
        IntArrayList temporalIndices = TimeUtils.getTemporalIndices(tempAnnotations);

        if (!tempAnnotations.isEmpty()) {
            IndexedWord relroot = this.getRelation().getHeadWord();
            if (relroot.index() > 0) {
                // TempEx words with 'tmod' typed dependency
                this.time.setTimeWithReln(sg, relroot, tempAnnotations, temporalIndices, EnglishGrammaticalRelations.TEMPORAL_MODIFIER);
                this.time.setTimeWithReln(sg, relroot, tempAnnotations, temporalIndices, EnglishGrammaticalRelations.ADVERBIAL_MODIFIER);
                this.time.setTimeWithReln(sg, relroot, tempAnnotations, temporalIndices, EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER);
                this.time.setTimeWithReln(sg, relroot, tempAnnotations, temporalIndices, EnglishGrammaticalRelations.XCLAUSAL_COMPLEMENT);
            }
        }

        // Drop the temporal words when needed from each rel-time
        for (Time rTime: this.time.getTimes()) {
            this.dropSpaTeWordsFromTriple(rTime.getTemporalWords(), sg);
        }

        // Detect "local" TempEx's (TempEx modifying a noun only)
        for (AnnotatedPhrase ap: this.triple) {
            ap.detectTime(tempAnnotations, sg);
        }
    }

    /**
     * Given a list of SpacEx expressions, and the dependency parse of the sentence, detect
     * the space of the annotated proposition (on triple's level)
     *
     * @param spatialAnnotations list of spatial annotations
     * @param sg dependency parse of the sentence (SemanticGraph object)
     */
    public void detectSpace(ObjectArrayList<Space> spatialAnnotations, SemanticGraph sg) {
        // Get the indices of the words
        IntArrayList spatialIndices = SpaceUtils.getSpatialIndices(spatialAnnotations);

        if (!spatialAnnotations.isEmpty()) {
            IndexedWord relroot = this.getRelation().getHeadWord();

            if (relroot.index() > 0) {
                this.space.setSpaceWithReln(sg, relroot, spatialAnnotations, spatialIndices, EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER);
            }
        }

        // Drop the temporal words when needed from each rel-time
        for (Space rSpace: this.space.getSpaces()) {
            this.dropSpaTeWordsFromTriple(rSpace.getSpatialWords(), sg);
        }

        // Detect "local" SpaceEx's (SpaceEx modifying a noun only)
        for (AnnotatedPhrase ap: this.triple) {
            ap.detectSpace(spatialAnnotations, sg);
        }
        // TODO: fix this!!!
        //detectPhraseSpaceWithPrep(spatialAnnotations, sg);

        // Detect space for "is in" implicit extractions
        if (this.getRelation().getWordList().size() == 2) {
            if (WordCollectionUtils.toLemmaString(this.getRelation().getWordList()).equals("be in")) {
                if (!this.getObject().getWordList().isEmpty()) {
                    if (WordCollectionUtils.isLocation(this.getObject().getWordList())) {
                        Space space = new Space();
                        space.setPredicate(this.getRelation().getWordList().get(1));
                        space.setAllSpatialWords(this.getObject().getWordList());
                        space.setCoreSpatialWords(this.getObject().getWordList());
                        this.space.addSpace(space);
                    }
                }
            }
        }
    }

    /**
     * Given a SpaTeCore object (containing temporal/spatial words) and the sentence semantic graph, drop the list of
     * temporal/spatial words in SpaTeCore from the triple's words
     * @param spaTe SpaTeCore object (temporal annotations for proposition)
     * @param sg sentence semantic graph
     */
    private void dropSpaTeWordsFromTriple(SpaTeCore spaTe, SemanticGraph sg) {
        // Nothing to drop if spaTe has an empty list of temporal/spatial words
        if (spaTe.getAllWords().isEmpty()) {
            return;
        }

        // Don't drop anything if the nAry constituents are triples themselves
        // (unless the word list is contained inside of the phrase, not exhausting it)
        if (this.nAryConstituents.size() <= 3) {
            for (AnnotatedPhrase aPhrase: this.triple) {
                if (aPhrase.containsAll(spaTe.getAllWords())) {
                    if (aPhrase.getWordList().size() > spaTe.getAllWords().size()) {
                        aPhrase.removeAll(spaTe.getAllWords());
                        if (spaTe.hasPredicate()) {
                            aPhrase.removeWord(spaTe.getPredicate());
                        }
                    }
                }
            }
        } else { // For n-ary constituents with size greater than 4;
            int spaTeTupleIndex = -1;
            for (int i = 0; i < this.nAryConstituents.size(); i++) {
                if (this.nAryConstituents.get(i).contains(spaTe.getAllWords().get(0))) {
                    spaTeTupleIndex = i;
                    break;
                }
            }

            // If the TempEx/SpacEx is not in the n-ary constituent, just annotate (nothing to drop)
            if (spaTeTupleIndex == -1) {
                return;
            } else if (spaTeTupleIndex == this.nAryConstituents.size() - 1) {
                this.nAryConstituents.remove(spaTeTupleIndex);

                Proposition prop = new Proposition();
                prop.setPhrases(this.nAryConstituents);
                prop.setNaryPhrases(this.nAryConstituents);
                prop.setConstituentTypes(this.nAryConstituentTypes);
                prop.convertNAryPropositionToCompactTriple();

                // Add the proposition from ClausIE to the list of propositions of MinIE
                ObjectArrayList<AnnotatedPhrase> annotatedTriple = new ObjectArrayList<>();
                for (int i = 0; i < prop.getConstituents().size(); i++){
                    AnnotatedPhrase aPhrase = new AnnotatedPhrase(prop.getConstituents().get(i));
                    aPhrase.detectQuantities(sg, i);
                    aPhrase.annotateQuantities();
                    annotatedTriple.add(aPhrase);
                }
                this.triple = annotatedTriple.clone();
                this.pushWordsToRelation();
            } else if (spaTeTupleIndex < this.nAryConstituents.size() - 1) {
                // If the TempEx is not in the last tuple from the n-ary tuple, drop it from the triple
                ObjectArrayList<IndexedWord> lastNAryPhrase = this.nAryConstituents.get(spaTeTupleIndex + 1).getWordList();

                // Don't drop anything if the last phrase from the n-ary tuples is a single word adverb
                if (lastNAryPhrase.size() == 1 && WordUtils.isAdverb(lastNAryPhrase.get(0))) {
                    return;
                }

                // In this case, the TempEx is within the relation or the object
                this.getRelation().removeAll(spaTe.getAllWords());
                this.getObject().removeAll(spaTe.getAllWords());
                if (spaTe.hasPredicate()) {
                    this.getRelation().removeWord(spaTe.getPredicate());
                    this.getObject().removeWord(spaTe.getPredicate());
                }
            }
        }
    }

    /**
     * Annotate the polarity for the triple
     * @param sg semantic graph of the sentence
     */
    public void annotatePolarity(SemanticGraph sg){
        Polarity pol;
        // Set polarity according to relations only
        // In some cases, there's only one word, in which case we don't drop anything
        if (this.getRelation().getWordList().size() == 1) {
            return;
        }

        pol = Polarity.getPolarity(this.getRelation());
        this.factuality.setPolarity(pol);

        // If the polarity is negative, drop the negative words
        this.getRelation().removeWordsFromList(pol.getNegativeWords());
    }

    /** Set the modality for all annotated propositions */
    public void annotateModality(SemanticGraph sg){
        Modality mod;
        // Set modality according to relations only
        // In some cases, there's only one word, in which case we don't drop anything
        if (this.getRelation().getWordList().size() == 1)
            return;

        mod = Modality.getModality(this.getRelation());
        this.setModality(mod);

        // If the modality is poss/cert, drop those words
        this.getRelation().removeWordsFromList(mod.getPossibilityWords());
        this.getRelation().removeWordsFromList(mod.getCertaintyWords());
    }

    /**
     * Given a proposition (list of annotated phrases), push words from objects to the relation if possible
     */
    public void pushWordsToRelation(){
        // AnnotatedProposition prop
        ObjectArrayList<IndexedWord> pushWords = new ObjectArrayList<>();

        if (this.triple.size() > 2 && this.getObject().getWordList().size() > 0){
            this.pushPrepositionsAndAdverbs(pushWords);

            // If we have TO+ VB* .* NP .* => push TO+ VB* to the relation
            this.pushInfinitiveVPs(pushWords);

            // Check for PPs with one of their NPs being a NER
            this.pushWordsFromPPsWithNERs(pushWords);

            this.pushNPfromPP(pushWords);
        }
    }

    /**
     * If the object starts with preposition, push prepositions (and adverbs modifying it)
     *
     * @param pushWords words to be pushed
     */
    private void pushPrepositionsAndAdverbs(ObjectArrayList<IndexedWord> pushWords) {
        IndexedWord firstObjectWord;
        firstObjectWord = this.getObject().getWordList().get(0);

        while ((firstObjectWord != null) &&
                (firstObjectWord.tag().equals(POS_TAG.IN) || firstObjectWord.tag().equals(POS_TAG.RB) ||
                        firstObjectWord.tag().equals(POS_TAG.WRB)) &&
                this.getObject().getWordList().size() > 1){

            // If it's an adverb, check if the adverb should be pushed
            if (firstObjectWord.tag().equals(POS_TAG.RB) && !this.pushAdverb(this.getObject())){
                break;
            }
            else
                pushWords.add(firstObjectWord);

            // Add the word to the end of the relation, and remove it from the object
            this.getRelation().addWordsToList(pushWords);
            this.getObject().removeWordsFromList(pushWords);

            if (this.getObject().getWordList().size() > 0){
                firstObjectWord = this.getObject().getWordList().get(0);
                pushWords.clear();
            }
            else
                firstObjectWord = null;
        }
    }

    /**
     * If we have NP_1 IN NP_2, but nothing else (no additional prepositions). Push NP_1 to relation
     *
     * @param pushWords words to be pushed to the relation
     */
    private void pushNPfromPP(ObjectArrayList<IndexedWord> pushWords) {
        if (WordCollectionUtils.countPrepositions(this.getObject().getWordList()) == 1){
            pushWords.clear();
            int prepIndex = -1;
            for (int i = 0; i < this.getObject().getWordList().size(); i++){
                if (this.getObject().getWordList().get(i).tag().equals(POS_TAG.IN) &&
                        this.getObject().getWordList().get(i).ner().equals(NE_TYPE.NO_NER)){
                    if (this.getObject().getWordList().get(i).index() ==
                            this.getObject().getWordList().get(this.getObject().getWordList().size() - 1).index()){
                        break;
                    }
                    prepIndex = i;
                    break;
                }
            }
            for (int i = 0; i <= prepIndex; i++){
                pushWords.add(this.getObject().getWordList().get(i));
            }
            // Add the word to the end of the relation, and remove it from the object
            this.getRelation().addWordsToList(pushWords);
            this.getObject().removeWordsFromList(pushWords);
            pushWords.clear();
        }
    }

    /**
     * If we have TO+ VB* .* NP .* => push TO+ VB* to the relation
     *
     * @param pushWords list of words to be pushed (here is the result)
     */
    private void pushInfinitiveVPs(ObjectArrayList<IndexedWord> pushWords) {
        // If we have TO+ VB* .* NP .* => push TO+ VB* to the relation
        TokenSequencePattern tPattern = TokenSequencePattern.compile(REGEX.T_TO_VP_IN);
        TokenSequenceMatcher tMatcher = tPattern.getMatcher(this.getObject().getWordCoreLabelList());
        while (tMatcher.find()){
            List<CoreMap> matches = tMatcher.groupNodes();

            // Check if the first word of the matches is the same as the first object word
            CoreLabel firstWord = new CoreLabel(matches.get(0));
            if (firstWord.index() != this.getObject().getWordList().get(0).index()) {
                break;
            }

            CoreLabel lastWord = new CoreLabel(matches.get(matches.size() - 1));
            for (CoreMap cm: matches){
                CoreLabel cl = new CoreLabel(cm);
                if (cl.ner().equals(NE_TYPE.NO_NER)){
                    // If adverb is not followed by preposition, don't push it
                    if (WordUtils.isAdverb(cl)){
                        if (cl.index() == lastWord.index()){
                            break;
                        }
                    }
                    // Don't push the last word of the object
                    if (this.getObject().getWordList().get(this.getObject().getWordList().size() -1).index() == cl.index())
                        break;
                    // Add the pushed words to the list
                    pushWords.add(new IndexedWord(cl));
                } else {
                    break;
                }
            }

            // Push the words, clear the list
            this.getRelation().addWordsToList(pushWords);
            this.getObject().removeWordsFromList(pushWords);
            pushWords.clear();
        }
        pushWords.clear();
    }

    /**
     *  Check for PPs with one of their NPs being a NER
     * @param pushWords words to be pushed to the relation
     */
    private void pushWordsFromPPsWithNERs(ObjectArrayList<IndexedWord> pushWords) {
        TokenSequencePattern tPattern = TokenSequencePattern.compile(REGEX.T_NP_IN_OPT_DT_RB_JJ_OPT_ENTITY);
        TokenSequenceMatcher tMatcher = tPattern.getMatcher(this.getObject().getWordCoreLabelList());
        while (tMatcher.find()){
            List<CoreMap> matches = tMatcher.groupNodes();
            CoreLabel firstWord = new CoreLabel(matches.get(0));
            if (firstWord.index() != this.getObject().getWordList().get(0).index())
                continue;

            CoreLabel prep = new CoreLabel();
            for (CoreMap cm: matches){
                CoreLabel cl = new CoreLabel(cm);
                if (!cl.tag().equals(POS_TAG.IN) && !cl.tag().equals(POS_TAG.TO)){
                    pushWords.add(new IndexedWord(cl));
                } else {
                    pushWords.add(new IndexedWord(cl));
                    prep = cl;
                    break;
                }
            }
            if (prep.ner().equals(NE_TYPE.NO_NER)){
                // Add the word to the end of the relation, and remove it from the object
                this.getRelation().addWordsToList(pushWords);
                this.getObject().removeWordsFromList(pushWords);
                pushWords.clear();
            }
        }
    }


    /**
     * Checks if the adverb(s) from the object should be pushed to the relation (if the adverb is followed by preposition
     * or 'to).
     *
     * @param object: a phrase, the object of the proposition
     * @return true, if an adverb is followed by a preposition or "to"
     */
    private boolean pushAdverb(Phrase object){
        TokenSequencePattern tPattern = TokenSequencePattern.compile(REGEX.T_RB_OPT_IN_TO_OPT);
        TokenSequenceMatcher tMatcher = tPattern.getMatcher(object.getWordCoreLabelList());
        while (tMatcher.find()){
            CoreLabel firstWordMatch = new CoreLabel(tMatcher.groupNodes().get(0));
            if (firstWordMatch.index() == object.getWordList().get(0).index() &&
                    object.getWordList().get(0).ner().equals(NE_TYPE.NO_NER)){
                return true;
            }
        }
        return false;
    }
}
