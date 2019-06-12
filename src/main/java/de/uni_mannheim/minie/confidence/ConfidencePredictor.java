package de.uni_mannheim.minie.confidence;


import de.uni_mannheim.clausie.ClausIE;
import de.uni_mannheim.minie.annotation.AnnotatedProposition;

import edu.stanford.nlp.ling.IndexedWord;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;

import org.jpmml.evaluator.*;
import org.jpmml.model.PMMLUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * @author Sebastian Wanner
 * @author Kiril Gashteovski
 */
public class ConfidencePredictor {
    private static final Logger logger = LoggerFactory.getLogger(ConfidencePredictor.class);
    private FeatureExtractor featureExtractor;
    private Evaluator evaluator;
    private ModelEvaluator<?> modelEvaluator;
    private ObjectArrayList<IndexedWord> sentence;

    public ConfidencePredictor(){
        featureExtractor = new FeatureExtractor();

        // load prediction model from pmml file
        logger.info("Loading Prediction Model ... ");
        PMML pmml = readPMML();
        logger.info("Prediction Model Loaded");

        // create prediction model
        ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
        modelEvaluator = modelEvaluatorFactory.newModelEvaluator(pmml);
        evaluator = modelEvaluator;

        evaluator.verify();
    }

    /**
     * initiate feature extraction and perform prediction
     * @param aProp: triple
     * @param clausie: Clausie object
     *  @param tempSentence: original sentence
     * @return confidencescore of the triple
     */

    public double prediction(AnnotatedProposition aProp, ClausIE clausie, ObjectArrayList<IndexedWord> tempSentence) throws IOException {
        Map features;
        double confidence = -1.0;

        if (this.sentence == null){
            this.sentence = tempSentence;
        }

        if (!this.sentence.equals(tempSentence)){
            featureExtractor.clearPropositionsDictMode();
            featureExtractor.clearPropositionsAggressiveMode();
            this.sentence = tempSentence;
        }

        //input for feature extraction
        featureExtractor.clear();
        featureExtractor.setSemGraph(clausie.getSemanticGraph());
        featureExtractor.setSentence(tempSentence);
        featureExtractor.setProposition(aProp);
        featureExtractor.setClause(aProp.getClause());
        featureExtractor.setClausIE(clausie);


        //extract FeatureExtractor from Sentence
        // long startTime = System.currentTimeMillis();
        features = featureExtractor.getFeaturesForPredictionModel();
        //long stopTime = System.currentTimeMillis();
        //System.out.println("Time for Feature Extraction: " + Long.toString(stopTime - startTime) );

        if (features != null){
            //predict
            //long startTime = System.currentTimeMillis();
            confidence = getConfidenceScore(features);
            //long stopTime = System.currentTimeMillis();
            //System.out.println("Time for Prediction: " + Long.toString(stopTime - startTime) );
        }

        return confidence;
    }

    /**
     * Perform the prediction in order to get the confidence Score
     * @param features: features from the featureExtractor
     * @return confidence score of the triple
     */
    private double getConfidenceScore(Map<String, Object> features) {
        Object targetFieldValue = null;
        Double confidence = -1.0;

        // prepare the arguments
        Map<FieldName, FieldValue> arguments = readArgumentsFromLine(features, modelEvaluator);

        // self test
        modelEvaluator.verify();

        // evaluate
        Map<FieldName, ?> results = evaluator.evaluate(arguments);

        // get results
        List<TargetField> targetFields = evaluator.getTargetFields();
        for(TargetField targetField : targetFields){
            FieldName targetFieldName = targetField.getName();

            targetFieldValue = results.get(targetFieldName);
        }

        // get Probabilities
        if (targetFieldValue instanceof HasProbability){
            HasProbability hasProbability = (HasProbability) targetFieldValue;
            confidence = hasProbability.getProbability("1");
        }

        return confidence;
    }

    /**
     * Convert features in a map objective as input for prediction model
     * @param map: features from the FeatureExtractor
     * @return Arguments for prediction model
     */

    private Map<FieldName, FieldValue> readArgumentsFromLine(Map<String, Object> map, Evaluator evaluator) {
        Map<FieldName, FieldValue> arguments = new LinkedHashMap<>();
        Object rawValue = null;

        List<InputField> inputFields = evaluator.getInputFields();

        for(InputField inputField : inputFields) {
            FieldName inputFieldName = inputField.getName();

            // The raw (ie. user-supplied) value could be any Java primitive value
            if(map.get(inputFieldName.toString()) instanceof Double){
                rawValue = map.get(inputFieldName.toString());
            }

            if(map.get(inputFieldName.toString()) instanceof String){
                rawValue = map.get(inputFieldName.toString());
            }

            // The raw value is passed through: 1) outlier treatment, 2) missing value treatment, 3) invalid value treatment and 4) type conversion
            FieldValue inputFieldValue = inputField.prepare(rawValue);

            arguments.put(inputFieldName, inputFieldValue);
        }

        return arguments;
    }

    /**
     * Load prediction model from pmml file
     * @return prediction model in pmml
     */
    private PMML readPMML() {
        InputStream is = null;
        try {
            is = this.getClass().getResource("/minie-resources/pipeline.pmml").openStream();
        } catch (IOException e) {
            logger.warn("IOException: ", e);
            e.printStackTrace();
        }

        PMML p = null;
        try {
            p = PMMLUtil.unmarshal(is);
        } catch (SAXException e) {
            logger.warn("SAXException: ", e);
        } catch (JAXBException e) {
            logger.warn("JAXBException: ", e);
        }

        return p;
    }
}