package de.uni_mannheim.minie;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import de.uni_mannheim.clausie.ClausIE;
import de.uni_mannheim.clausie.clause.Clause;
import de.uni_mannheim.clausie.proposition.Proposition;
import de.uni_mannheim.constant.NE_TYPE;
import de.uni_mannheim.constant.POS_TAG;
import de.uni_mannheim.constant.SEPARATOR;
import de.uni_mannheim.minie.annotation.AnnotatedPhrase;
import de.uni_mannheim.minie.annotation.AnnotatedProposition;
import de.uni_mannheim.minie.annotation.Attribution;
import de.uni_mannheim.minie.annotation.SpaTe.time.Time;
import de.uni_mannheim.minie.annotation.factuality.Factuality;
import de.uni_mannheim.minie.annotation.factuality.Modality;
import de.uni_mannheim.minie.annotation.factuality.Polarity;
import de.uni_mannheim.minie.annotation.SpaTe.space.Space;
import de.uni_mannheim.minie.confidence.ConfidencePredictor;
import de.uni_mannheim.minie.minimize.object.ObjAggressiveMinimization;
import de.uni_mannheim.minie.minimize.object.ObjDictionaryMinimization;
import de.uni_mannheim.minie.minimize.object.ObjSafeMinimization;
import de.uni_mannheim.minie.minimize.relation.RelAggressiveMinimization;
import de.uni_mannheim.minie.minimize.relation.RelDictionaryMinimization;
import de.uni_mannheim.minie.minimize.relation.RelSafeMinimization;
import de.uni_mannheim.minie.minimize.subject.SubjAggressiveMinimization;
import de.uni_mannheim.minie.minimize.subject.SubjDictionaryMinimization;
import de.uni_mannheim.minie.minimize.subject.SubjSafeMinimization;
import de.uni_mannheim.minie.proposition.ImplicitExtractions;

import de.uni_mannheim.utils.coreNLP.WordCollectionUtils;
import de.uni_mannheim.utils.phrase.PhraseUtils;
import de.uni_mannheim.utils.Dictionary;
import de.uni_mannheim.utils.coreNLP.CoreNLPUtils;

import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphEdge;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static de.uni_mannheim.minie.annotation.Attribution.generateAccording;

/**
 * @author Kiril Gashteovski
 */
public class MinIE {
    private static final Logger logger = LoggerFactory.getLogger(MinIE.class);
    /** List of annotated propositions **/
    private ObjectArrayList<AnnotatedProposition> propositions;

    /** The semantic graph of the whole sentence **/
    private SemanticGraph sentenceSemGraph;

    /** The whole sentence as a list of indexed words **/
    private ObjectArrayList<IndexedWord> sentence;

    /** List of temporal annotations (for the whole sentence) **/
    private ObjectArrayList<Time> tempAnnotations;

    /** List of spatial annotations (for the whole sentence) **/
    private ObjectArrayList<Space> spaceAnnotations;

    /** Reusability variables **/
    private ObjectOpenHashSet<String> propsWithAttribution;

    /** Confidence Predictor **/
    private ConfidencePredictor confidencePredictor;

    /** Constructor **/
    public MinIE(ObjectArrayList<AnnotatedProposition> props){
        this.propositions = props;
    }

    /** MinIE mode **/
    public enum Mode {
        AGGRESSIVE,
        DICTIONARY,
        SAFE,
        COMPLETE
    }

    /** Default constructor **/
    public MinIE(){
        this.propositions = new ObjectArrayList<>();
        this.sentenceSemGraph = new SemanticGraph();
        this.sentence = new ObjectArrayList<>();
        this.propsWithAttribution = new ObjectOpenHashSet<>();
        this.tempAnnotations = new ObjectArrayList<>();
        this.spaceAnnotations = new ObjectArrayList<>();
        this.confidencePredictor = new ConfidencePredictor();
    }

    /**
     * @param sentence - input sentence
     * @param parser - dependency parse pipeline of the sentence
     * @param mode - the minimization mode
     * @param d - dictionary of multi-word expressions (for MinIE-D)
     */
    public MinIE(String sentence, StanfordCoreNLP parser, Mode mode, Dictionary d) {
        // Initializations
        this.propositions = new ObjectArrayList<>();
        this.sentenceSemGraph = new SemanticGraph();
        this.sentence = new ObjectArrayList<>();
        this.propsWithAttribution = new ObjectOpenHashSet<>();
        this.tempAnnotations = new ObjectArrayList<>();
        this.spaceAnnotations = new ObjectArrayList<>();

        this.minimize(sentence, parser, mode, d);
    }

    /**
     * @param sentence - input sentence
     * @param parser - dependency parse pipeline of the sentence
     * @param mode - the minimization mode
     *
     * NOTE: If mode is MinIE-D, then this will proceed as MinIE-D but with empty dictionary
     * (i.e. will drop every word that is a candidate)
     */
    public MinIE(String sentence, StanfordCoreNLP parser, Mode mode) {
        this.propositions = new ObjectArrayList<>();
        this.sentenceSemGraph = new SemanticGraph();
        this.sentence = new ObjectArrayList<>();
        this.propsWithAttribution = new ObjectOpenHashSet<>();
        this.tempAnnotations = new ObjectArrayList<>();
        this.spaceAnnotations = new ObjectArrayList<>();

        this.minimize(sentence, parser, mode, new Dictionary());
    }

    /**
     * @param sg - dependency parse graph of the sentence
     * @param mode - the minimization mode
     * @param calculateConfScore - if true: calculate the confidence score
     *
     * NOTE: If mode is MinIE-D, then this will proceed as MinIE-D but with empty dictionary
     * (i.e. will drop every word that is a candidate)
     */
    public MinIE(SemanticGraph sg, Mode mode, boolean calculateConfScore) {
        this.propositions = new ObjectArrayList<>();
        this.sentenceSemGraph = new SemanticGraph();
        this.sentence = new ObjectArrayList<>();
        this.propsWithAttribution = new ObjectOpenHashSet<>();
        this.tempAnnotations = new ObjectArrayList<>();
        this.spaceAnnotations = new ObjectArrayList<>();

        this.minimize(sg, mode, new Dictionary(), calculateConfScore);
    }

    /**
     * @param sg - dependency parse graph of the sentence
     * @param mode - the minimization mode
     * @param dict - dictionary of multi-word expressions (for MinIE-D)
     * @param calculateConfScore - if true: calculate the confidence score of the triples
     */
    public MinIE(SemanticGraph sg, Mode mode, Dictionary dict, boolean calculateConfScore) {
        this.propositions = new ObjectArrayList<>();
        this.sentenceSemGraph = new SemanticGraph();
        this.sentence = new ObjectArrayList<>();
        this.propsWithAttribution = new ObjectOpenHashSet<>();
        this.tempAnnotations = new ObjectArrayList<>();
        this.spaceAnnotations = new ObjectArrayList<>();


        this.minimize(sg, mode, dict, calculateConfScore);
    }

    /**
     * Given an input sentence, parser, mode and a dictionary, make extractions and then minimize them accordingly.
     * The parsing occurs INSIDE this function.
     *
     * @param sentence - input sentence
     * @param parser - dependency parse pipeline for the sentence
     * @param mode - minimization mode
     * @param d - dictionary (for MinIE-D)
     */
    public void minimize(String sentence, StanfordCoreNLP parser, Mode mode, Dictionary d) {
        // Run ClausIE first
        ClausIE clausie = new ClausIE(CoreNLPUtils.parse(parser, sentence));

        // Start minimizing by annotating
        this.setSemanticGraph(clausie.getSemanticGraph());
        this.setPropositions(clausie);
        this.annotateSpaceTime();
        this.annotatePolarity();
        this.annotateModality();

        // Minimize according to the modes (COMPLETE mode doesn't minimize)
        if (mode == Mode.SAFE)
            this.minimizeSafeMode();
        else if (mode == Mode.DICTIONARY)
            this.minimizeDictionaryMode(d.words());
        else if (mode == Mode.AGGRESSIVE)
            this.minimizeAggressiveMode();

        this.removeDuplicates();
        this.pruneAnnotatedPropositions();
    }

    private void annotateSpaceTime() {
        for (AnnotatedProposition aProp: this.propositions) {
            aProp.detectTime(this.tempAnnotations, this.sentenceSemGraph);
            aProp.detectSpace(this.spaceAnnotations, this.sentenceSemGraph);
        }
    }

    /**
     * Given an input sentence, dependency parse, mode and a dictionary, make extractions and then minimize them accordingly.
     * The parsing occurs OUTSIDE this function.
     *
     * @param sg - semantic graph object (dependency parse of the sentence)
     * @param mode - minimization mode
     * @param d - dictionary (for MinIE-D)
     * @param calculateConfScore - if true: calculate the confidence score of the triple
     */
    public void minimize(SemanticGraph sg, Mode mode, Dictionary d, boolean calculateConfScore) {
        // Run ClausIE first
        ClausIE clausie = new ClausIE(sg);

        // Start minimizing by annotating
        this.setSemanticGraph(sg);
        this.setPropositions(clausie);
        this.annotateSpaceTime();
        this.annotatePolarity();
        this.annotateModality();

        // Minimize according to the modes (COMPLETE mode doesn't minimize)
        if (mode == Mode.SAFE)
            this.minimizeSafeMode();
        else if (mode == Mode.DICTIONARY)
            this.minimizeDictionaryMode(d.words());
        else if (mode == Mode.AGGRESSIVE)
            this.minimizeAggressiveMode();

        this.removeDuplicates();
        if (calculateConfScore) {
            this.predictConfidenceScore(clausie);
        }
        this.pruneAnnotatedPropositions();
    }

    public void clear(){
        this.propositions.clear();
        this.sentenceSemGraph = null;
        this.sentence.clear();
        this.propsWithAttribution.clear();
        this.tempAnnotations.clear();
    }

    public ObjectArrayList<AnnotatedProposition> getPropositions(){
        return this.propositions;
    }
    public AnnotatedProposition getProposition(int i){
        return this.propositions.get(i);
    }
    public SemanticGraph getSentenceSemanticGraph(){
        return this.sentenceSemGraph;
    }
    public AnnotatedPhrase getSubject(int i){
        return this.propositions.get(i).getSubject();
    }
    public AnnotatedPhrase getRelation(int i){
        return this.propositions.get(i).getRelation();
    }
    public AnnotatedPhrase getObject(int i){
        return this.propositions.get(i).getObject();
    }

    public void setPropositions(ObjectArrayList<AnnotatedProposition> props){
        this.propositions = props;
    }
    public void setTempAnnotations(ObjectArrayList<Time> tAnnotations) {
        this.tempAnnotations = tAnnotations;
    }
    public void setSpaceAnnotations(ObjectArrayList<Space> sAnnotations) {
        this.spaceAnnotations = sAnnotations;
    }

    /**
     * Given a proposition, detect the attribution. Returns true if a attribution was detected and false otherwise.
     * @param proposition input proposition
     */
    private boolean detectAttribution(Proposition proposition){
        // If the proposition is of size 2, return (nothing to detect here, it's an SV)
        if (proposition.getConstituents().size() < 3) {
            return false;
        }

        // Attribution flag is set to 'false' by default
        boolean attributionDetected = false;

        // Reusable variables
        ClausIE clausieObj = new ClausIE();
        StringBuilder sb = new StringBuilder();
        ObjectArrayList<IndexedWord> tempListOfWords = new ObjectArrayList<>();

        // Elements of the triple
        AnnotatedPhrase subject = new AnnotatedPhrase(proposition.subject());
        AnnotatedPhrase relation = new AnnotatedPhrase(proposition.relation());
        AnnotatedPhrase object = new AnnotatedPhrase(proposition.object());

        // Get the root and if it's null, return 'true'
        relation.setHeadWord(CoreNLPUtils.getHeadWord(relation.getWordList(), this.sentenceSemGraph));
        IndexedWord root = relation.getHeadWord();
        if (root == null) return true;

        // Detect "according to..." patterns by checking the adverbials (i.e. the objects)
        if (object.getWordList().size() > 2){
            if (object.getWordList().get(0).word().toLowerCase().equals(Attribution.ACCORDING) &&
                    object.getWordList().get(1).tag().equals(POS_TAG.TO)){
                tempListOfWords.clear();
                tempListOfWords.addAll(subject.getWordList());
                tempListOfWords.addAll(relation.getWordList());
                SemanticGraph newsg = CoreNLPUtils.getSubgraphFromWords(this.sentenceSemGraph, tempListOfWords);

                // The attribution predicate "according to"
                sb.append(object.getWordList().get(1).word());
                Factuality factuality = new Factuality(new Polarity(Polarity.Type.POSITIVE), new Modality(Modality.Type.CERTAINTY));
                this.generatePropositionsWithAttribution(clausieObj, newsg, new Attribution(
                        new AnnotatedPhrase(object.getWordSubList(2, object.getWordList().size()-1)),
                        factuality,
                        generateAccording()));
                sb.setLength(0);
                attributionDetected = true;
            }
        }

        // Modality and polarity of the attribution (detecting attribution with predicates)
        Polarity.Type pol = Polarity.Type.POSITIVE;
        Modality.Type mod = null;
        IndexedWord relHead = relation.getHeadWord();
        if (Modality.VERB_CERTAINTY.contains(relHead.lemma().toLowerCase())){
            // By default, the modality is CERTAINTY unless proven otherwise
            mod = Modality.Type.CERTAINTY;

            // If the head verb of the relation is negated, set polarity to NEGATIVE
            if (sentenceSemGraph.getChildWithReln(relHead, EnglishGrammaticalRelations.NEGATION_MODIFIER) != null){
                pol = Polarity.Type.NEGATIVE;
            }

            // If there is a modal verb as a modifier of the head verb, make it a possibility modality type
            Set<IndexedWord> auxs = sentenceSemGraph.getChildrenWithReln(relHead, EnglishGrammaticalRelations.AUX_MODIFIER);
            if (!auxs.isEmpty()){
                for (IndexedWord w: auxs){
                    if (w.tag().equals(POS_TAG.MD)){
                        mod = Modality.Type.POSSIBILITY;
                    }
                }
            }
        } else if (Modality.VERB_POSSIBILITY.contains(relHead.lemma().toLowerCase())){
            mod = Modality.Type.POSSIBILITY;

            // If the head verb of the relation is negated, set polarity to NEGATIVE
            if (sentenceSemGraph.getChildWithReln(relHead, EnglishGrammaticalRelations.NEGATION_MODIFIER) != null){
                pol = Polarity.Type.NEGATIVE;
            }

            // If there is a modal verb as a modifier of the head verb, make it a possibility modality type
            Set<IndexedWord> auxs = sentenceSemGraph.getChildrenWithReln(relHead, EnglishGrammaticalRelations.AUX_MODIFIER);
            if (!auxs.isEmpty()){
                for (IndexedWord w: auxs){
                    if (w.tag().equals(POS_TAG.MD)){
                        mod = Modality.Type.POSSIBILITY;
                    }
                }
            }
        }


        // If a predicate is found
        List<SemanticGraphEdge> nsubjs;
        List<IndexedWord> nsubjChildren;
        if (mod != null){
            // Stop searching if there's no verb in the object
            if (!WordCollectionUtils.hasVerb(object.getWordList())){
                return false;
            }

            // Get the subject relationships
            nsubjs = this.sentenceSemGraph.findAllRelns(EnglishGrammaticalRelations.NOMINAL_SUBJECT);
            nsubjs.addAll(this.sentenceSemGraph.findAllRelns(EnglishGrammaticalRelations.CLAUSAL_SUBJECT));
            nsubjs.addAll(this.sentenceSemGraph.findAllRelns(EnglishGrammaticalRelations.SUBJECT));
            nsubjs.addAll(this.sentenceSemGraph.findAllRelns(EnglishGrammaticalRelations.CLAUSAL_COMPLEMENT));

            nsubjChildren = new ArrayList<>();
            for (SemanticGraphEdge e: nsubjs){
                nsubjChildren.add(e.getDependent());
            }

            // Iterate through the subjects
            Factuality factuality = new Factuality(new Polarity(pol), new Modality(mod));
            SemanticGraph objSg;
            Attribution tempAttr;
            for (IndexedWord child: nsubjChildren){
                // Process only the ones that have verbs in the object
                if (WordCollectionUtils.hasVerb(object.getWordList()) && object.getWordList().contains(child)){
                    objSg = CoreNLPUtils.getSubgraphFromWords(this.sentenceSemGraph, object.getWordList());
                    tempAttr = new Attribution(subject, factuality, relation.getHeadWord());
                    tempAttr.detectTime(this.tempAnnotations, this.sentenceSemGraph);
                    tempAttr.detectSpace(this.spaceAnnotations, this.sentenceSemGraph);
                    this.generatePropositionsWithAttribution(clausieObj, objSg, tempAttr);
                    attributionDetected = true;
                }
            }
        }

        return attributionDetected;
    }

    /**
     * Given a ClausIE object, semantic graph object and a attribution, make new extractions from the object,
     * add them in the list of propositions and add the attribution as well.
     *
     * @param clausieObj: ClausIE object (reusable variable)
     * @param objSg: semantic graph object of the object
     * @param a: the attribution
     */
    private void generatePropositionsWithAttribution(ClausIE clausieObj, SemanticGraph objSg, Attribution a){
        // New clausie object
        clausieObj.clear();
        clausieObj.setSemanticGraph(objSg);
        clausieObj.detectClauses();
        clausieObj.generatePropositions(clausieObj.getSemanticGraph());

        // Reusable variable for annotated phrases
        AnnotatedPhrase aPhrase;

        for (Clause c: clausieObj.getClauses()){
            for (Proposition p: c.getPropositions()){
                // Add the proposition from ClausIE to the list of propositions of MinIE
                ObjectArrayList<AnnotatedPhrase> prop = new ObjectArrayList<>();
                for (int i = 0; i < p.getConstituents().size(); i++){
                    aPhrase = new AnnotatedPhrase(p.getConstituents().get(i));
                    aPhrase.detectQuantities(this.sentenceSemGraph, i);
                    aPhrase.annotateQuantities();
                    prop.add(aPhrase);
                }
                if (this.pruneAnnotatedProposition(prop))
                    continue;
                AnnotatedProposition aProp = new AnnotatedProposition(prop, new Attribution(a));
                aProp.pushWordsToRelation();
                aProp.setnAryConstituents(p.getNaryPhrases());
                aProp.setnAryConstituentTypes(p.getConstituentTypes());
                aProp.setClause(c);
                aProp.setExtractionType(c.getExtractionType());
                this.propositions.add(aProp);
                this.propsWithAttribution.add(PhraseUtils.listOfAnnotatedPhrasesToString(prop));
            }
        }
    }

    private void pushWordsToRelationsInPropositions() {
        for (AnnotatedProposition proposition : this.propositions) {
            proposition.pushWordsToRelation();
        }
    }

    /**
     * Given a ClausIE object, set the prepositions from ClausIE to MinIE (don't annotate neg. and poss.)
     * While assigning the propositions, these are things that are done:
     *  * detect attributions
     *  * push words to the relation (if possible)
     * @param clausie: clausie object containing clause types, propositions, sentence dependency parse, ...
     */
    public void setPropositions(ClausIE clausie){
        // Set of strings for propositions with attribution
        this.propsWithAttribution = new ObjectOpenHashSet<>();
        StringBuffer sb = new StringBuffer();

        // Set the sentence, make the implicit extractions from it, and add them to the list of propositions
        this.sentence = new ObjectArrayList<> (clausie.getSemanticGraph().vertexListSorted());
        ImplicitExtractions extractions = new ImplicitExtractions(this.sentence, this.sentenceSemGraph);
        extractions.generateImplicitExtractions();
        int id = 0;
        for (AnnotatedProposition aProp: extractions.getImplicitExtractions()) {
            if (this.pruneAnnotatedProposition(aProp.getTriple())) {
                continue;
            }
            id++;
            aProp.setId(id);
            this.propositions.add(aProp);
        }

        // Set the propositions extracted from ClausIE to MinIE
        for (Clause clause: clausie.getClauses()){
            for (Proposition proposition: clause.getPropositions()){
                id++;
                AnnotatedProposition aProp = this.annotateProposition(proposition, id);

                if (aProp != null) {
                    if (this.pruneAnnotatedProposition(aProp.getTriple())) {
                        continue;
                    }

                    //set clause
                    aProp.setClause(clause);
                    aProp.setExtractionType(clause.getExtractionType());
                    this.propositions.add(aProp);
                }
            }
        }

        // Remove proposiions which have no attributions, but they have duplicate propositions having an attribution
        this.removeDuplicateFactsWithoutAttribution(sb);
    }

    /**
     * Given a proposition and an ID, restructure and annotate the proposition
     *
     * @param proposition proposition to be annotated
     * @param id ID of the proposition
     *
     * @return annotated proposition
     */
    public AnnotatedProposition annotateProposition(Proposition proposition, int id) {
        // If a attribution is detected, add the content of the proposition to the list
        boolean attributionDetected = this.detectAttribution(proposition);

        // Don't add the proposition if an attribution is detected or its content has an attribution already
        if (attributionDetected || this.propsWithAttribution.contains(proposition.propositionToString())) {
            return null;
        }

        // Add the proposition from ClausIE to the list of propositions of MinIE
        ObjectArrayList<AnnotatedPhrase> prop = new ObjectArrayList<>();
        for (int i = 0; i < proposition.getConstituents().size(); i++){
            AnnotatedPhrase aPhrase = new AnnotatedPhrase(proposition.getConstituents().get(i));
            aPhrase.detectQuantities(this.sentenceSemGraph, i);
            aPhrase.annotateQuantities();
            prop.add(aPhrase);
        }

        // Prune proposition if needed
        if (this.pruneAnnotatedProposition(prop)){
            return null;
        }

        //Annotated proposition
        AnnotatedProposition aProp = new AnnotatedProposition(prop, id);

        // Set the n-ary constituents
        aProp.setnAryConstituents(proposition.getNaryPhrases());
        aProp.setnAryConstituentTypes(proposition.getConstituentTypes());

        // Push words to relation
        aProp.pushWordsToRelation();

        // Reset the head words of the phrases
        aProp.getSubject().setHeadWord(CoreNLPUtils.getHeadWord(aProp.getSubject().getWordList(), this.sentenceSemGraph));
        aProp.getRelation().setHeadWord(CoreNLPUtils.getHeadWord(aProp.getRelation().getWordList(), this.sentenceSemGraph));
        aProp.getObject().setHeadWord(CoreNLPUtils.getHeadWord(aProp.getObject().getWordList(), this.sentenceSemGraph));

        return aProp;
    }

    /**
     * Remove proposiions which have no attributions, but they have duplicate propositions having an attribution
     */
    private void removeDuplicateFactsWithoutAttribution(StringBuffer sb) {
        // TODO: temporary solution, make this in removeDuplicates()
        ObjectArrayList<AnnotatedProposition> delProps = new ObjectArrayList<>();
        ObjectOpenHashSet<String> propWithAttributions = new ObjectOpenHashSet<>();
        String thisProp;
        for (AnnotatedProposition proposition : this.propositions) {
            thisProp = PhraseUtils.listOfAnnotatedPhrasesToString(proposition.getTriple());
            // Remove proposiions which have no attributions, but they have duplicate propositions having an attribution
            if (this.propsWithAttribution.contains(thisProp)) {
                if (proposition.getAttribution().getAttributionPhrase() == null) {
                    delProps.add(proposition);
                } else {
                    sb.append(thisProp);
                    sb.append(SEPARATOR.SPACE);
                    sb.append(proposition.getAttribution().toString());
                    if (propWithAttributions.contains(sb.toString())) {
                        delProps.add(proposition);
                    } else {
                        propWithAttributions.add(sb.toString());
                    }
                    sb.setLength(0);
                }
            }
        }
        this.propositions.removeAll(delProps);
    }

    public void removeDuplicates(){
        ObjectOpenHashSet<String> propStrings = new ObjectOpenHashSet<>();
        ObjectOpenHashSet<String> propStringPS = new ObjectOpenHashSet<>();

        ObjectArrayList<AnnotatedProposition> remProps = new ObjectArrayList<>();
        String propString;
        for (AnnotatedProposition prop: this.propositions){
            if (prop.getModality().getModalityType() == Modality.Type.POSSIBILITY) {
                propStringPS.add(prop.wordsToString());
            }
            propString = prop.toString();
            if (propStrings.contains(propString)) {
                remProps.add(prop);
            }
            else {
                propStrings.add(propString);
            }
        }

        // Remove PS duplicates
        for (AnnotatedProposition prop: this.propositions) {
            if (prop.getModality().getModalityType() == Modality.Type.CERTAINTY) {
                if (propStringPS.contains(prop.wordsToString())) {
                    remProps.add(prop);
                }
            }
        }

        // Also, remove the ones with empty object
        for (AnnotatedProposition proposition : this.propositions) {
            if (proposition.getSubject().getWordList().isEmpty())
                remProps.add(proposition);

            if (proposition.getTriple().size() == 3)
                if (proposition.getObject().getWordList().isEmpty())
                    remProps.add(proposition);
        }

        this.propositions.removeAll(remProps);
    }

    /**
     * Given a proposition, check if it should be pruned or not.
     * @param proposition input proposition
     * @return true, if the proposition should be pruned, false otherwise
     */
    private boolean pruneAnnotatedProposition(ObjectArrayList<AnnotatedPhrase> proposition){
        AnnotatedPhrase subj = proposition.get(0);
        AnnotatedPhrase rel = proposition.get(1);

        if (!WordCollectionUtils.hasVerb(rel.getWordList()))
            return true;
        if (subj.getWordList().isEmpty())
            return true;

        if (proposition.size() == 3){
            AnnotatedPhrase obj = proposition.get(2);
            if (obj.getWordList().isEmpty()) {
                return true;
            }

            IndexedWord w = obj.getWordList().get(obj.getWordList().size()-1);

            if (w.tag().equals(POS_TAG.IN) && w.ner().equals(NE_TYPE.NO_NER))
                return true;

            if (obj.getWordList().size() == 1){
                if (w.tag().equals(POS_TAG.IN) || w.tag().equals(POS_TAG.TO)){
                    return true;
                }
            }

            if (w.tag().equals(POS_TAG.WDT) || w.tag().equals(POS_TAG.WP) ||
                    w.tag().equals(POS_TAG.WP_P) || w.tag().equals(POS_TAG.WRB)){
                return true;
            }

            if (this.detectClauseModifier(proposition)){
                return true;
            }

            if ((rel.getWordList().size() == 1)) {
                if (rel.getWordList().get(0).lemma().equals("be")) {
                    if (subj.isOneNER() && obj.isOneNER()) {
                        if (!obj.getWordList().get(0).ner().equals(NE_TYPE.MISC)) {
                            return true;
                        }
                    }
                }
            }
        }

        return false;
    }

    public void pruneAnnotatedPropositions() {
        ObjectArrayList<AnnotatedProposition> remprops = new ObjectArrayList<>();
        for (AnnotatedProposition aProp: this.propositions) {
            if (this.pruneAnnotatedProposition(aProp.getTriple())) {
                remprops.add(aProp);
            }
        }
        this.propositions.removeAll(remprops);
    }

    /**
     * Given an annotated proposition, check if it contains a clause modifier as an object. If so, return 'true', else
     * return 'false'
     * @param proposition: annotated proposition
     * @return 'true' if the object is a clause modifier; 'false' otherwise
     */
    private boolean detectClauseModifier(ObjectArrayList<AnnotatedPhrase> proposition){
        if (WordCollectionUtils.hasVerb(proposition.get(2).getWordList())){
            for (IndexedWord word: proposition.get(2).getWordList()){
                if (this.sentenceSemGraph.getParent(word) != null){
                    SemanticGraphEdge edge = this.sentenceSemGraph.getEdge(this.sentenceSemGraph.getParent(word), word);
                    if ((edge.getRelation() == EnglishGrammaticalRelations.SUBJECT) ||
                            (edge.getRelation() == EnglishGrammaticalRelations.NOMINAL_SUBJECT) ||
                            (edge.getRelation() == EnglishGrammaticalRelations.CLAUSAL_SUBJECT) ||
                            (edge.getRelation() == EnglishGrammaticalRelations.NOMINAL_PASSIVE_SUBJECT)){
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /** Sets the polarity of each annotated proposition **/
    public void annotatePolarity(){
        for (AnnotatedProposition proposition : this.propositions) {
            proposition.annotatePolarity(this.sentenceSemGraph);
        }
    }

    /** Set the modality for all annotated propositions */
    public void annotateModality(){
        // Set modality according to relations only
        for (AnnotatedProposition proposition : this.propositions) {
            proposition.annotateModality(this.sentenceSemGraph);
        }
    }

    public void setSemanticGraph(SemanticGraph sg){
        this.sentenceSemGraph = sg;
    }

    /** Dictionary mode minimization **/
    public void minimizeDictionaryMode(ObjectOpenHashSet<String> collocations){
        for (int i = 0; i < this.propositions.size(); i++){
            SubjDictionaryMinimization.minimizeSubject(this.getSubject(i), this.sentenceSemGraph, collocations);
            RelDictionaryMinimization.minimizeRelation(this.getRelation(i), this.sentenceSemGraph, collocations);
            ObjDictionaryMinimization.minimizeObject(this.getObject(i), this.sentenceSemGraph, collocations);
        }
        this.pushWordsToRelationsInPropositions();
    }

    /** Safe mode minimization **/
    public void minimizeSafeMode(){
        for (int i = 0; i < this.propositions.size(); i++){
            SubjSafeMinimization.minimizeSubject(this.getSubject(i), this.sentenceSemGraph);
            RelSafeMinimization.minimizeRelation(this.getRelation(i), this.sentenceSemGraph);
            ObjSafeMinimization.minimizeObject(this.getObject(i), this.sentenceSemGraph);
        }
        this.pushWordsToRelationsInPropositions();
    }

    /** Aggressive mode minimization **/
    public void minimizeAggressiveMode() {
        for (int i = 0; i < this.propositions.size(); i++) {
            SubjAggressiveMinimization.minimizeSubject(this.getSubject(i), this.sentenceSemGraph);
            RelAggressiveMinimization.minimizeRelation(this.getRelation(i), this.sentenceSemGraph);
            ObjAggressiveMinimization.minimizeObject(this.getObject(i), this.sentenceSemGraph);
        }
        this.pushWordsToRelationsInPropositions();
    }

    /**
     * predict the confidence score of a triple
     * @param clausie: are used for feature occursInDictMode
     */
    public void predictConfidenceScore(ClausIE clausie) {
        for (int i = 0; i < this.propositions.size(); i++){
            double confidenceScore;

            // predict Confidence
            try {
                confidenceScore =  this.confidencePredictor.prediction(this.propositions.get(i), clausie, this.sentence);
                this.propositions.get(i).setConfidence(confidenceScore);
            } catch (IOException e) {
                logger.warn("IOException: ", e);
                this.propositions.get(i).setConfidence(-1.0);
            }
        }
    }
}
