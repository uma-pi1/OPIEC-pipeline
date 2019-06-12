package de.uni_mannheim.minie.annotation.SpaTe.space;

import de.uni_mannheim.constant.NE_TYPE;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.semgraph.SemanticGraph;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.List;

/**
 * @author Kiril Gashteovski
 */

public class SpaceUtils {
    public static IntArrayList getSpatialIndices(ObjectArrayList<Space> spatialAnnotations) {
        IntArrayList spatialIndices = new IntArrayList();
        if (!spatialAnnotations.isEmpty()) {
            for (Space s : spatialAnnotations) {
                for (IndexedWord w : s.getCoreSpatialWords()) {
                    spatialIndices.add(w.index());
                }
            }
        }

        return spatialIndices;
    }

    public static ObjectArrayList<Space> annotateSpace(SemanticGraph sg) {
        ObjectArrayList<Space> spaces = new ObjectArrayList<>();
        List<IndexedWord> words = sg.vertexListSorted();
        Space s = new Space();
        boolean checkLoc = true;
        for (int i = 0; i < words.size(); i++) {
            if (words.get(i).ner().equals(NE_TYPE.LOCATION)) {
                if (!checkLoc) {
                    if (!s.getCoreSpatialWords().isEmpty()) {
                        spaces.add(s);
                    }
                    s = new Space();
                }
                checkLoc = true;
                s.addSpatialWord(words.get(i));
            } else {
                checkLoc = false;
            }
        }
        if (!s.getCoreSpatialWords().isEmpty()) {
            spaces.add(s);
        }

        return spaces;
    }
}
