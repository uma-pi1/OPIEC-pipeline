package de.uni_mannheim.minie.annotation.SpaTe.time;

import de.uni_mannheim.constant.NE_TYPE;
import de.uni_mannheim.minie.annotation.SpaTe.SpaTeCore;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.AnnotationPipeline;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.time.SUTime;
import edu.stanford.nlp.time.TimeAnnotations;
import edu.stanford.nlp.time.TimeExpression;
import edu.stanford.nlp.time.Timex;
import edu.stanford.nlp.util.CoreMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.List;

/**
 * @author Kiril Gashteovski
 */
public class TimeUtils {
    public static IntArrayList getTemporalIndices(ObjectArrayList<Time> tempAnnotations) {
        IntArrayList temporalIndices = new IntArrayList();
        if (!tempAnnotations.isEmpty()) {
            for (Time ta : tempAnnotations) {
                for (IndexedWord w : ta.getCoreTemporalWords()) {
                    temporalIndices.add(w.index());
                }
            }
        }

        return temporalIndices;
    }

    public static ObjectArrayList<Time> annotateTime(String sentence, AnnotationPipeline temporalPipeline, String refDate) {
        Annotation annotation = new Annotation(sentence);
        annotation.set(CoreAnnotations.DocDateAnnotation.class, refDate);
        temporalPipeline.annotate(annotation);

        ObjectArrayList<Time> timexAnnsAll = new ObjectArrayList<>();
        ObjectArrayList<IndexedWord> temporalWords = new ObjectArrayList<>();
        Time.Timex3Type timex3Type;

        for (CoreMap cm: annotation.get(TimeAnnotations.TimexAnnotations.class)) {
            List<CoreLabel> tokens = cm.get(CoreAnnotations.TokensAnnotation.class);
            for (CoreLabel cl: tokens) {
                temporalWords.add(new IndexedWord(cl));
            }

            SUTime.Temporal temporal = cm.get(TimeExpression.Annotation.class).getTemporal();
            Timex timex = cm.get(TimeAnnotations.TimexAnnotation.class);

            if (timex.timexType().equals(NE_TYPE.DATE)) {
                timex3Type = Time.Timex3Type.DATE;
            } else if (timex.timexType().equals(NE_TYPE.TIME)) {
                timex3Type = Time.Timex3Type.TIME;
            } else if (timex.timexType().equals(NE_TYPE.DURATION)) {
                timex3Type = Time.Timex3Type.DURATION;
            } else if (timex.timexType().equals(NE_TYPE.SET)) {
                timex3Type = Time.Timex3Type.SET;
            } else {
                timex3Type = Time.Timex3Type.UNKNOWN;
            }

            Time ta = new Time();
            ta.setId(timex.tid());
            ta.setType(timex3Type);
            ta.setTimex3mod(temporal.getMod());
            ta.setTempWords(new SpaTeCore(temporalWords));
            ta.setTimex3Xml(timex.toXmlElement());
            ta.setValue(timex.value());

            timexAnnsAll.add(ta);
            temporalWords.clear();
        }
        return timexAnnsAll;
    }
}
