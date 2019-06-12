package de.uni_mannheim.minie.annotation.SpaTe.time;

import de.uni_mannheim.constant.POS_TAG;
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
 * List of times and ways to detect and handle relational times for propositions
 *
 * @author Kiril Gashteovski
 */
public class PropTime {
    /** List of RelTime objects **/
    private TreeSet<Time> times;

    public PropTime() {
        this.times = new TreeSet<>();
    }

    public boolean isEmpty() {
        return this.times.isEmpty();
    }
    public int size() {
        return this.times.size();
    }
    public TreeSet<Time> getTimes() {
        return times;
    }

    /**
     * Check if 'time' is in the list of times
     * @param time a temporal object
     * @return true: if 'time' is in the list of time objects, false otherwise
     */
    public boolean containsTime(Time time) {
        for (Time t: this.times) {
            if (t.isEqual(time)) {
                return true;
            }
        }
        return false;
    }

    public void addTime(Time t) {
        this.times.add(t);
    }

    /**
     * Given the dependency parse, the relation root, the token indices of the TempEx and the grammatical relation,
     * set the time given the specific grammatical relation (so far: tmod, prep and advmod)
     *
     * @param sg dependency parse of the sentence
     * @param relroot the head word of the relation
     * @param temporalIndices the indices of the tokens of the TempEx
     * @param gr grammatical relation between a pair of words (i.e. typed dependency)
     */
    public void setTimeWithReln(SemanticGraph sg, IndexedWord relroot, ObjectArrayList<Time> times,
                                IntArrayList temporalIndices, GrammaticalRelation gr) {
        Set<IndexedWord> tempwords = sg.getChildrenWithReln(relroot, gr);
        if (!tempwords.isEmpty()) {
            // Get temp roots  (include verb from xcomp if present)
            Set<IndexedWord> relroots = new HashSet<>();
            relroots.add(relroot);
            for (IndexedWord w: sg.getChildrenWithReln(relroot, EnglishGrammaticalRelations.XCLAUSAL_COMPLEMENT)) {
                if (WordUtils.isVerb(w)) {
                    relroots.add(w);
                }
            }

            // For each "relhead' in "relroots", set temporal annotations if found
            for (IndexedWord relhead: relroots) {
                ObjectArrayList<IndexedWord> temproots = this.getTempRoots(sg, relhead, temporalIndices, gr);

                if (temproots.isEmpty()) {
                    continue;
                }

                for (IndexedWord root: temproots) {
                    // Flags
                    boolean tmodChecks = temporalIndices.contains(root.index()) && gr.equals(EnglishGrammaticalRelations.TEMPORAL_MODIFIER);
                    boolean advmodChecks = temporalIndices.contains(root.index()) && gr.equals(EnglishGrammaticalRelations.ADVERBIAL_MODIFIER);
                    boolean prepChecks = gr.equals(EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER);
                    boolean xcompChecks = gr.equals(EnglishGrammaticalRelations.XCLAUSAL_COMPLEMENT);

                    if (tmodChecks || advmodChecks || prepChecks || xcompChecks) {
                        Time ct = this.getTempExWords(sg, root, times);

                        // Add time only if it's not in the list
                        Time pTime = new Time(ct);
                        if (prepChecks) {
                            pTime.setPredicate(sg.getParent(root));
                        }
                        if (xcompChecks) {
                            IndexedWord tempParent = sg.getParent(root);
                            if (tempParent.tag().equals(POS_TAG.IN)) {
                                pTime.setPredicate(tempParent);
                            }
                        }
                        if (!this.containsTime(pTime)) {
                            if (!pTime.getCoreTemporalWords().isEmpty())
                                this.addTime(pTime);
                        }
                    }
                }
            }
        }
    }

    /**
     * @param sg semantic graph of the sentence
     * @param relroot relation root
     * @param temporalIndices the TempEx indices
     * @param gr grammatical relation
     * @return temp root; node governing with tmod typed dependency
     */
    private ObjectArrayList<IndexedWord> getTempRoots(SemanticGraph sg, IndexedWord relroot, IntArrayList temporalIndices, GrammaticalRelation gr) {
        ObjectArrayList<IndexedWord> temproots = new ObjectArrayList<>();
        if (gr.equals(EnglishGrammaticalRelations.TEMPORAL_MODIFIER)) {
            temproots.addAll(sg.getChildrenWithReln(relroot, gr));
        } else if (gr.equals(EnglishGrammaticalRelations.ADVERBIAL_MODIFIER)) {
            temproots.addAll(sg.getChildrenWithReln(relroot, gr));
        } else if (gr.equals(EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER)) {
            Set<IndexedWord> preps = sg.getChildrenWithReln(relroot, gr);
            for (IndexedWord prep : preps) {
                List<IndexedWord> children = sg.getChildList(prep);
                for (IndexedWord w : children) {
                    if (temporalIndices.contains(w.index())) {
                        temproots.add(w);
                    }
                }
            }
        } else if (gr.equals(EnglishGrammaticalRelations.XCLAUSAL_COMPLEMENT)) {
            temproots.addAll(sg.getChildrenWithReln(relroot, EnglishGrammaticalRelations.TEMPORAL_MODIFIER));

            if (temproots.isEmpty()) {
                Set<IndexedWord> preps = sg.getChildrenWithReln(relroot, EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER);
                for (IndexedWord prep : preps) {
                    List<IndexedWord> children = sg.getChildList(prep);
                    for (IndexedWord w : children) {
                        if (temporalIndices.contains(w.index())) {
                            temproots.add(w);
                        }
                    }
                }

                for (IndexedWord w: sg.getChildrenWithReln(relroot, EnglishGrammaticalRelations.ADVERBIAL_MODIFIER)) {
                    if (temporalIndices.contains(w.index())) {
                        temproots.add(w);
                    }
                }
            }
        }

        return temproots;
    }

    /**
     * @param sg semantic graph of the sentence
     * @param temproot the temporal root
     * @return the TempEx list of words
     */
    private Time getTempExWords(SemanticGraph sg, IndexedWord temproot, ObjectArrayList<Time> coreTimes) {
        Collection<GrammaticalRelation> tabuRels = new ArrayList<>();
        tabuRels.add(EnglishGrammaticalRelations.RELATIVE_CLAUSE_MODIFIER);
        tabuRels.add(EnglishGrammaticalRelations.PUNCTUATION);
        tabuRels.add(EnglishGrammaticalRelations.APPOSITIONAL_MODIFIER);
        tabuRels.add(EnglishGrammaticalRelations.valueOf("dep"));

        tabuRels.add(EnglishGrammaticalRelations.COORDINATION);
        tabuRels.add(EnglishGrammaticalRelations.CONJUNCT);
        tabuRels.add(EnglishGrammaticalRelations.VERBAL_MODIFIER);

        // Index of the core time where the relroot is
        int ind = -1;
        for (int i = 0; i < coreTimes.size(); i++) {
            IntArrayList coreIndices = WordCollectionUtils.getWordIndices(coreTimes.get(i).getCoreTemporalWords());
            if (coreIndices.contains(temproot.index())) {
                ind = i;
                break;
            }
        }

        // Return empty Time object in case the temproot is not found in the list of CoreTimes
        Time currentCoreTime = new Time();
        if (ind == -1) {
            return currentCoreTime;
        } else {
            currentCoreTime = coreTimes.get(ind);
        }

        // Get all the words containing a temporal expression
        Set<IndexedWord> tempwordsSet = SemanticGraphUtils.descendantsTabuRelns(sg, temproot, tabuRels);
        tempwordsSet.add(temproot);
        ObjectArrayList<IndexedWord> sortedTempWords = WordCollectionUtils.getSortedWords(tempwordsSet);
        IntArrayList sortedTempWordsIndices = WordCollectionUtils.getWordIndices(sortedTempWords);

        // Remove the words not appearing in sortedTempWords
        for (IndexedWord w: currentCoreTime.getCoreTemporalWords().clone()) {
            if (!sortedTempWordsIndices.contains(w.index())) {
                currentCoreTime.removeTemporalWord(w);
            }
        }

        // Get Time object and add pre/post-modifiers
        Time ct = new Time(currentCoreTime);
        ct.setAllTemporalWords(sortedTempWords);
        IntArrayList ctCoreIndices = WordCollectionUtils.getWordIndices(ct.getCoreTemporalWords());
        IntArrayList ctPreModIndices = WordCollectionUtils.getWordIndices(ct.getPreMods());
        IntArrayList ctPostModIndices = WordCollectionUtils.getWordIndices(ct.getPostMods());

        for (IndexedWord w: sortedTempWords) {
            if (ctCoreIndices.contains(w.index())) {
                continue;
            } else {
                if (w.index() < ctCoreIndices.getInt(0)) {
                    if (!ctPreModIndices.contains(w.index())) {
                        ct.addPreModWord(w);
                    }
                } else {
                    if (!ctPostModIndices.contains(w.index())) {
                        ct.addPostModWord(w);
                    }
                }
            }
        }

        return ct;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Time:(");
        for (Time rt: this.times) {
            sb.append(rt.toString());
        }
        sb.append(")");
        return sb.toString();
    }
}
