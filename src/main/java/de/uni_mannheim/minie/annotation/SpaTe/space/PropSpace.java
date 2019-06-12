package de.uni_mannheim.minie.annotation.SpaTe.space;

import de.uni_mannheim.utils.coreNLP.WordCollectionUtils;
import de.uni_mannheim.utils.coreNLP.WordUtils;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphUtils;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import edu.stanford.nlp.trees.GrammaticalRelation;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.*;

/**
 * List of spaces and ways to detect and handle spaces for propositions
 *
 * @author Kiril Gashteovski
 */
public class PropSpace {
    /** List of space objects **/
    private TreeSet<Space> spaces;

    public PropSpace() {
        this.spaces = new TreeSet<>();
    }

    public boolean isEmpty() {
        return this.spaces.isEmpty();
    }
    public int size() {
        return this.spaces.size();
    }
    public TreeSet<Space> getSpaces() {
        return spaces;
    }

    public void setSpaceWithReln(SemanticGraph sg, IndexedWord relroot, ObjectArrayList<Space> spatialAnnotations,
                                 IntArrayList spatialIndices, GrammaticalRelation gr) {
        Set<IndexedWord> spacewords = sg.getChildrenWithReln(relroot, gr);
        if (!spacewords.isEmpty()) {
            Set<IndexedWord> relroots = new HashSet<>();
            relroots.add(relroot);
            for (IndexedWord w: sg.getChildrenWithReln(relroot, EnglishGrammaticalRelations.XCLAUSAL_COMPLEMENT)) {
                if (WordUtils.isVerb(w)) {
                    relroots.add(w);
                }
            }

            for (IndexedWord relhead: relroots) {
                ObjectArrayList<IndexedWord> spaceroots = this.getSpaceRoots(sg, relhead, spatialIndices, gr);

                if (spaceroots.isEmpty()) {
                    continue;
                }

                for (IndexedWord root: spaceroots) {
                    boolean prepChecks = gr.equals(EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER);

                    if (prepChecks) {
                        Space sp = this.getSpatExWords(sg, root, spatialAnnotations);

                        Space pSpace = new Space(sp);

                        if (prepChecks) {
                            pSpace.setPredicate(sg.getParent(root));
                        }

                        if (pSpace.getCoreSpatialWords().isEmpty()) {
                            continue;
                        }

                        if (!this.containsSpace(pSpace)) {
                            this.addSpace(pSpace);
                        }
                    }
                }
            }
        }
    }

    /**
     * @param sg semantic graph of the sentence
     * @param relroot relation root
     * @param spatialIndices the SpaTEX indices
     * @param gr grammatical relation
     *
     * @return spatial roots
     */
    private ObjectArrayList<IndexedWord> getSpaceRoots(SemanticGraph sg, IndexedWord relroot, IntArrayList spatialIndices, GrammaticalRelation gr) {
        ObjectArrayList<IndexedWord> spatialroots = new ObjectArrayList<>();
        if (gr.equals(EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER)) {
            Set<IndexedWord> preps = sg.getChildrenWithReln(relroot, gr);
            for (IndexedWord prep : preps) {
                List<IndexedWord> children = sg.getChildList(prep);
                for (IndexedWord w : children) {
                    if (spatialIndices.contains(w.index())) {
                        spatialroots.add(w);
                    }
                }
            }
        }
        return spatialroots;
    }

    /**
     * @param sg semantic graph of the sentence
     * @param spatialroot the spatial root
     * @return the SpatEx list of words
     */
    private Space getSpatExWords(SemanticGraph sg, IndexedWord spatialroot, ObjectArrayList<Space> spaces) {
        Collection<GrammaticalRelation> tabuRels = new ArrayList<>();
        tabuRels.add(EnglishGrammaticalRelations.RELATIVE_CLAUSE_MODIFIER);
        tabuRels.add(EnglishGrammaticalRelations.PUNCTUATION);
        tabuRels.add(EnglishGrammaticalRelations.APPOSITIONAL_MODIFIER);
        tabuRels.add(EnglishGrammaticalRelations.valueOf("dep"));

        tabuRels.add(EnglishGrammaticalRelations.COORDINATION);
        tabuRels.add(EnglishGrammaticalRelations.CONJUNCT);
        tabuRels.add(EnglishGrammaticalRelations.VERBAL_MODIFIER);

        // Index of the core space where the relroot is
        int ind = -1;
        for (int i = 0; i < spaces.size(); i++) {
            if (spaces.get(i).getCoreSpatialWords().contains(spatialroot)) {
                ind = i;
                break;
            }
        }

        // Return empty Space object in case the spatialroot is not found in the list of Spaces
        Space currentSpace = new Space();
        if (ind == -1) {
            return currentSpace;
        } else {
            currentSpace = spaces.get(ind);
        }

        // Get all the words containing a spatial expression
        Set<IndexedWord> spatialwordsSet = SemanticGraphUtils.descendantsTabuRelns(sg, spatialroot, tabuRels);
        spatialwordsSet.add(spatialroot);
        ObjectArrayList<IndexedWord> sortedSpatialWords = WordCollectionUtils.getSortedWords(spatialwordsSet);

        // Remove the words not appearing in sortedSpatialWords
        for (IndexedWord w: currentSpace.getCoreSpatialWords().clone()) {
            if (!sortedSpatialWords.contains(w)) {
                currentSpace.removeSpatialWord(w);
            }
        }

        // Get Space object and add pre/post-modifiers
        Space sp = new Space(currentSpace);
        sp.setAllSpatialWords(sortedSpatialWords);
        for (IndexedWord w: sortedSpatialWords) {
            if (sp.getCoreSpatialWords().contains(w)) {
                continue;
            } else {
                if (w.index() < sp.getCoreSpatialWords().get(0).index()) {
                    if (!sp.getPreMods().contains(w)) {
                        sp.addPreModWord(w);
                    }
                } else {
                    if (!sp.getPostMods().contains(w)) {
                        sp.addPostModWord(w);
                    }
                }
            }
        }

        return sp;
    }

    /**
     * Check if 'space' is in the list of spaces
     * @param space a spatial object
     * @return true: if 'space' is in the list of space objects, false otherwise
     */
    public boolean containsSpace(Space space) {
        for (Space s: this.spaces) {
            if (s.isEqual(space)) {
                return true;
            }
        }
        return false;
    }

    public void addSpace(Space s) {
        this.spaces.add(s);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Space:(");
        for (Space rs: this.spaces) {
            sb.append(rs.toString());
        }
        sb.append(")");
        return sb.toString();
    }
}
