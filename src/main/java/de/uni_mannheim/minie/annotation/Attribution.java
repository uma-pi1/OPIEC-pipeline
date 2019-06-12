package de.uni_mannheim.minie.annotation;

import de.uni_mannheim.constant.CHARACTER;
import de.uni_mannheim.constant.SEPARATOR;

import de.uni_mannheim.minie.annotation.SpaTe.space.PropSpace;
import de.uni_mannheim.minie.annotation.SpaTe.space.Space;
import de.uni_mannheim.minie.annotation.SpaTe.space.SpaceUtils;
import de.uni_mannheim.minie.annotation.factuality.Factuality;
import de.uni_mannheim.minie.annotation.factuality.Modality;
import de.uni_mannheim.minie.annotation.factuality.Polarity;
import de.uni_mannheim.minie.annotation.SpaTe.time.Time;
import de.uni_mannheim.minie.annotation.SpaTe.time.PropTime;
import de.uni_mannheim.minie.annotation.SpaTe.time.TimeUtils;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

/**
 * A class representing the attribution
 *
 * @author Kiril Gashteovski
 */
public class Attribution {
    /** A phrase containing the words for the attribution **/
    private AnnotatedPhrase attributionPhrase;
    /** The factuality of the attribution (possibility or certainty) **/
    private Factuality factuality;
    /** The predicate verb (as a string in its lemma version) **/
    private IndexedWord predicateVerb;
    /** Temporal annotation for the attribution **/
    private PropTime time;
    /** Spatial annotation for the attribution **/
    private PropSpace space;
    
    /** Some string constants necessary for detecting the attribution **/
    public static String ACCORDING = "according";
    
    /** Default constructor: modality == certainty, polarity == positive, attributionPhrase == null */
    public Attribution(){
        this.attributionPhrase = null;
        this.factuality = new Factuality();
        this.predicateVerb = new IndexedWord();
        this.time = new PropTime();
        this.space = new PropSpace();
    }

    /**
     * Parameterized constructor (without temporal annotation)
     *
     * @param attributionPhrase the attribution phrase
     * @param f factuality
     * @param pVerb predicate verb
     */
    public Attribution(AnnotatedPhrase attributionPhrase, Factuality f, IndexedWord pVerb){
        this.attributionPhrase = attributionPhrase;
        this.factuality = f;
        this.predicateVerb = pVerb;
        this.time = new PropTime();
        this.space = new PropSpace();
    }

    /** Copy constructor **/
    public Attribution(Attribution a){
        this.attributionPhrase = a.getAttributionPhrase();
        this.factuality = a.getFactuality();
        this.predicateVerb = a.getPredicateVerb();
        this.time = a.getTime();
        this.space = a.getSpace();
    }
    
    // Getters
    public AnnotatedPhrase getAttributionPhrase(){
        return this.attributionPhrase;
    }
    public Modality.Type getModalityType(){
        return this.factuality.getModalityType();
    }
    public Polarity.Type getPolarityType(){
        return this.factuality.getPolarityType();
    }
    public IndexedWord getPredicateVerb(){
        return this.predicateVerb;
    }
    public Factuality getFactuality() {
        return this.factuality;
    }
    public PropTime getTime() {
        return this.time;
    }
    public PropSpace getSpace() {
        return space;
    }

    // Clear the attribution
    public void clear(){
        this.attributionPhrase = new AnnotatedPhrase();
        this.factuality.clear();
        this.predicateVerb = new IndexedWord();
        this.time = new PropTime();
        this.space = new PropSpace();
    }

    /**
     * Given a list of temporal annotations and the semantic graph of the sentence, detect time
     * @param tempAnnotations
     * @param sg
     */
    public void detectTime(ObjectArrayList<Time> tempAnnotations, SemanticGraph sg) {
        // Get the indices of the words
        IntArrayList temporalIndices = TimeUtils.getTemporalIndices(tempAnnotations);

        if (!tempAnnotations.isEmpty()) {
            this.time.setTimeWithReln(sg, this.predicateVerb, tempAnnotations, temporalIndices, EnglishGrammaticalRelations.TEMPORAL_MODIFIER);
            this.time.setTimeWithReln(sg, this.predicateVerb, tempAnnotations, temporalIndices, EnglishGrammaticalRelations.ADVERBIAL_MODIFIER);
            this.time.setTimeWithReln(sg, this.predicateVerb, tempAnnotations, temporalIndices, EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER);
            this.time.setTimeWithReln(sg, this.predicateVerb, tempAnnotations, temporalIndices, EnglishGrammaticalRelations.XCLAUSAL_COMPLEMENT);
        }
    }

    /**
     * Given a list of spatial annotations and the semantic graph of the sentence, detect space
     * @param spatialAnnotations spatial annotations
     * @param sg semantic graph of the sentence
     */
    public void detectSpace(ObjectArrayList<Space> spatialAnnotations, SemanticGraph sg) {
        // Get the indices of the words
        IntArrayList temporalIndices = SpaceUtils.getSpatialIndices(spatialAnnotations);

        if (!spatialAnnotations.isEmpty()) {
            this.space.setSpaceWithReln(sg, this.predicateVerb, spatialAnnotations, temporalIndices, EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER);
            this.space.setSpaceWithReln(sg, this.predicateVerb, spatialAnnotations, temporalIndices, EnglishGrammaticalRelations.XCLAUSAL_COMPLEMENT);
        }
    }
    
    // Write down the attribution in the format (attribution_phrase, predicate, polarity, modality)
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(CHARACTER.LPARENTHESIS);

        // Append the attribution phrase
        for (int i = 0; i < this.attributionPhrase.getWordList().size(); i++) {
            sb.append(this.attributionPhrase.getWordList().get(i).word());
            if (i < this.attributionPhrase.getWordList().size() - 1)
                sb.append(SEPARATOR.SPACE);
        }

        sb.append(SEPARATOR.COMMA);
        sb.append(SEPARATOR.SPACE);
        
        // Append the predicate verb
        sb.append("Predicate: ");
        if (this.predicateVerb.lemma().equals("according")) {
            sb.append("according to");
        } else {
            sb.append(this.predicateVerb.lemma());
        }
        sb.append(SEPARATOR.COMMA);
        sb.append(SEPARATOR.SPACE);
        
        // Append the polarity
        sb.append("POLARITY:  ");
        if (this.factuality.getPolarityType() == Polarity.Type.POSITIVE)
            sb.append(Polarity.ST_POSITIVE);
        else 
            sb.append(Polarity.ST_NEGATIVE);
        sb.append(SEPARATOR.SPACE);
        sb.append(SEPARATOR.COMMA);
        sb.append(SEPARATOR.SPACE);
        
        // Append the modality
        sb.append("MODALITY:  ");
        if (this.factuality.getModalityType() == Modality.Type.CERTAINTY) {
            sb.append(Modality.ST_CERTAINTY);
        } else {
            sb.append(Modality.ST_POSSIBILITY);
        }

        // Append time if present
        if (!this.time.isEmpty()) {
            sb.append(SEPARATOR.SPACE);
            sb.append(this.time.toString());
        }

        // Append space if present
        if (!this.space.isEmpty()) {
            sb.append(SEPARATOR.SPACE);
            sb.append(this.space.toString());
        }

        sb.append(CHARACTER.RPARENTHESIS);

        return sb.toString().trim();
    }
    
    /** Return the attribution as a string in format "(Attribution Phrase, (POLARITY, MODALITY)) **/
    public String toStringCompact() {
        StringBuilder sb = new StringBuilder();
        sb.append(CHARACTER.LPARENTHESIS);

        // Append the attribution phrase
        for (int i = 0; i < this.attributionPhrase.getWordList().size(); i++) {
            sb.append(this.attributionPhrase.getWordList().get(i).word());
            if (i < this.attributionPhrase.getWordList().size() - 1)
                sb.append(SEPARATOR.SPACE);
        }
        
        sb.append(SEPARATOR.COMMA);
        sb.append(SEPARATOR.SPACE);
        
        // Append the factuality
        sb.append(CHARACTER.LPARENTHESIS);
        if (this.factuality.getPolarityType() == Polarity.Type.POSITIVE) {
            sb.append(Polarity.ST_PLUS);
        } else {
            sb.append(Polarity.ST_MINUS);
        }
        sb.append(SEPARATOR.COMMA);
        if (this.factuality.getModalityType() == Modality.Type.CERTAINTY) {
            sb.append(Modality.ST_CT);
        } else {
            sb.append(Modality.ST_PS);
        }
        sb.append(CHARACTER.RPARENTHESIS);
        sb.append(CHARACTER.RPARENTHESIS);
        
        return sb.toString();
    }

    public static IndexedWord generateAccording() {
        IndexedWord w = new IndexedWord();
        w.setWord("according");
        w.setLemma("according");
        w.setIndex(-2);
        w.setNER("O");
        w.setTag("VBG");
        return w;
    }
}
