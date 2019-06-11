package de.uni_mannheim.utils.coreNLP;

import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.time.TimeAnnotator;

import java.util.Properties;

/**
 * @author Kiril Gashteovski
 */
public class NLPPipeline {
    /**
     * Initializes and returns StanfordCoreNLP pipeline
     *
     * @return StanfordCoreNLP pipeline
     */
    public static StanfordCoreNLP StanfordDepNNParser(){
        Properties props = new Properties();

        props.put("language", "english");
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, depparse");
        props.put("depparse.model", "edu/stanford/nlp/models/parser/nndep/english_SD.gz");
        props.put("parse.originalDependencies", true);

        return new StanfordCoreNLP(props);
    }

    /**
     * Initialize SuTime pipeline
     *
     * @return initialized SuTime pipeline
     */
    public static AnnotationPipeline initTimePipeline() {
        Properties props = new Properties();
        AnnotationPipeline pipeline = new AnnotationPipeline();
        pipeline.addAnnotator(new TokenizerAnnotator(false));
        pipeline.addAnnotator(new WordsToSentencesAnnotator(false));
        pipeline.addAnnotator(new POSTaggerAnnotator(false));
        pipeline.addAnnotator(new TimeAnnotator("sutime", props));

        return pipeline;
    }
}
