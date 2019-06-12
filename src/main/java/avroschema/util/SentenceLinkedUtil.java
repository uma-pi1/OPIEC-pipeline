package avroschema.util;

import avroschema.linked.SentenceLinked;
import avroschema.linked.TokenLinked;

import de.uni_mannheim.constant.CHARACTER;
import de.uni_mannheim.constant.NE_TYPE;
import de.uni_mannheim.minie.annotation.SpaTe.time.Time;
import de.uni_mannheim.minie.annotation.SpaTe.time.TimeUtils;

import edu.stanford.nlp.pipeline.AnnotationPipeline;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

/**
 * @author Kiril Gashteovski
 */
public class SentenceLinkedUtil {
    /**
     * Given a sentence (withLinkedTokens, return the string of the sentence
     */
    public static String getSentenceLinkedString(SentenceLinked s){
        StringBuilder sbSentence = new StringBuilder();
        for (TokenLinked t: s.getTokens()){
            sbSentence.append(t.getWord());
            sbSentence.append(CHARACTER.SPACE);
        }

        return sbSentence.toString().trim();
    }

    /**
     * Given a sentence, temporal pipeline and a reference time, see if there is a
     * @param s: sentence
     * @param temporalPipeline: temporal pipeline
     * @param reftime: reference time string
     * @return true -> if there is some sort of temporal annotations in the sentence; false -> otherwise
     */
    public static boolean hasTime(SentenceLinked s, AnnotationPipeline temporalPipeline, String reftime) {
        ObjectArrayList<Time> tempAnnotations = TimeUtils.annotateTime(SentenceLinkedUtil.getSentenceLinkedString(s), temporalPipeline, reftime);
        return !tempAnnotations.isEmpty();
    }

    /**
     * Given a sentence, check if there are some spatial annotations (by checking the NER for LOCATION)
     * @param s: sentence
     * @return true: if some of the tokens contains the NER LOCATION; false - otherwise
     */
    public static boolean hasLocation(SentenceLinked s) {
        for (TokenLinked t: s.getTokens()) {
            if (t.getNer().equals(NE_TYPE.LOCATION)) {
                return true;
            }
        }
        return false;
    }
}
