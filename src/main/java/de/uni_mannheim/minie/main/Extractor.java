package de.uni_mannheim.minie.main;

import de.uni_mannheim.clausie.ClausIE;
import de.uni_mannheim.minie.MinIE;
import de.uni_mannheim.minie.annotation.AnnotatedProposition;
import de.uni_mannheim.minie.annotation.SpaTe.space.Space;
import de.uni_mannheim.minie.annotation.SpaTe.space.SpaceUtils;
import de.uni_mannheim.minie.annotation.SpaTe.time.Time;
import de.uni_mannheim.minie.annotation.SpaTe.time.TimeUtils;
import de.uni_mannheim.utils.Dictionary;
import de.uni_mannheim.utils.coreNLP.NLPPipeline;
import de.uni_mannheim.utils.minie.Utils;
import edu.stanford.nlp.pipeline.AnnotationPipeline;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import de.uni_mannheim.utils.coreNLP.CoreNLPUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;

/**
 * This class acts as a generic interface to the MinIE system
 *
 * @author Martin Achenbach
 * @author Kiril Gashteovski
 */
public class Extractor {
    private StanfordCoreNLP parser;
    private AnnotationPipeline temporalPipeline;
    private ClausIE clausIE;
    private MinIE minIE;
    private Dictionary dictionary;
    private String refdate;

    /**
     * default constructor
     */
    public Extractor() {
        // initialize the pipelines
        this.parser = NLPPipeline.StanfordDepNNParser();
        this.temporalPipeline = NLPPipeline.initTimePipeline();

        // initialize ClausIE
        this.clausIE = new ClausIE();

        // initialize MinIE
        this.minIE = new MinIE();

        // set up default dictionary
        try {
            this.setDictionary(new Dictionary(Utils.DEFAULT_DICTIONARIES));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * constructor with dictionary, helpful when running in dictionary mode
     * @param dictionary: dictionary
     */
    public Extractor(Dictionary dictionary) {
        // initialize the parser
        this.parser = NLPPipeline.StanfordDepNNParser();

        // initialize ClausIE
        this.clausIE = new ClausIE();

        // initialize MinIE
        this.minIE = new MinIE();

        // set dictionary
        this.setDictionary(dictionary);
    }

    /**
     * Set the reference date for the temporal annotations
     * @param rDate reference date for the temporal annotations
     */
    public void setRefdate(String rDate) {
        this.refdate = rDate;
    }

    /**
     * set the dictionary for dictionary mode
     * @param dictionary: dictionary to use
     */
    public void setDictionary(Dictionary dictionary) {
        this.dictionary = dictionary;
    }

    /**
     * analyze a sentence using a specific mode
     * @param sentence: sentence to analyze
     * @param mode: MinIE mode
     * @return the results of MinIE
     */
    public MinIE analyzeSentence(String sentence, MinIE.Mode mode) {
        // First reset objects
        this.clausIE.clear();
        this.minIE.clear();

        // Parse the sentence
        this.clausIE.setSemanticGraph(CoreNLPUtils.parse(this.parser, sentence));
        // Detect clauses
        this.clausIE.detectClauses();
        // Generate propositions
        this.clausIE.generatePropositions(this.clausIE.getSemanticGraph());

        // Start minimizing
        this.minIE.setSemanticGraph(this.clausIE.getSemanticGraph());
        this.minIE.setPropositions(this.clausIE);
        this.minIE.annotatePolarity();
        this.minIE.annotateModality();

        // Detect time and space
        ObjectArrayList<Time> tempAnnotations = TimeUtils.annotateTime(sentence, this.temporalPipeline, this.refdate);
        ObjectArrayList<Space> spaceAnnotations = SpaceUtils.annotateSpace(this.clausIE.getSemanticGraph());
        this.minIE.setTempAnnotations(tempAnnotations);
        this.minIE.setSpaceAnnotations(spaceAnnotations);

        // Minimize in given mode
        switch (mode) {
            case AGGRESSIVE:
                this.minIE.minimize(this.clausIE.getSemanticGraph(), MinIE.Mode.AGGRESSIVE, null, false);
                break;
            case DICTIONARY:
                this.minIE.minimize(this.clausIE.getSemanticGraph(), MinIE.Mode.DICTIONARY, this.dictionary, false);
                break;
            case SAFE:
                this.minIE.minimize(this.clausIE.getSemanticGraph(), MinIE.Mode.SAFE, null, false);
                break;
            case COMPLETE:
                break;
        }
        // Remove duplicates
        this.minIE.removeDuplicates();

        return this.minIE;
    }
}
