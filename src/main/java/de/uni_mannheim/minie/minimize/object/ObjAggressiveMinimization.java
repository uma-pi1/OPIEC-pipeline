package de.uni_mannheim.minie.minimize.object;

import java.util.HashSet;
import java.util.Set;

import de.uni_mannheim.constant.POS_TAG;
import de.uni_mannheim.constant.REGEX;
import de.uni_mannheim.minie.annotation.AnnotatedPhrase;
import de.uni_mannheim.minie.annotation.Quantity;

import de.uni_mannheim.utils.coreNLP.WordCollectionUtils;
import de.uni_mannheim.utils.coreNLP.WordUtils;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.ling.tokensregex.TokenSequenceMatcher;
import edu.stanford.nlp.ling.tokensregex.TokenSequencePattern;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import edu.stanford.nlp.trees.GrammaticalRelation;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import de.uni_mannheim.utils.coreNLP.CoreNLPUtils;

/**
 * @author Kiril Gashteovski
 */
public class ObjAggressiveMinimization {
    /**
     * Object aggressive minimization
     * @param object: object phrase
     * @param sg: semantic graph of the sentence
     */
    public static void minimizeObject(AnnotatedPhrase object, SemanticGraph sg){
        // Don't minimize if the phrase contains one word or no  words (rare cases)
        if (object.getWordList() == null || object.getWordList().size() <= 1){
            return;
        }
        
        // Don't minimize if the phrase is a multi word NER or multiple nouns in a sequence
        String seqPosNer = WordCollectionUtils.wordsToPosMergedNerSeq(object.getWordList());
        if (seqPosNer.matches(REGEX.MULTI_WORD_ENTITY) || seqPosNer.matches(REGEX.MULTI_WORD_NOUN)){
            return;
        }
        
        // Do safe minimization first
        ObjSafeMinimization.minimizeObject(object, sg);
        
        // List of words to be dropped
        ObjectArrayList<IndexedWord> dropWords = new ObjectArrayList<>();
        
        // Drop some type of modifiers 
        Set<GrammaticalRelation> excludeRels = new HashSet<>();
        excludeRels.add(EnglishGrammaticalRelations.ADVERBIAL_MODIFIER);
        excludeRels.add(EnglishGrammaticalRelations.ADJECTIVAL_MODIFIER);
        excludeRels.add(EnglishGrammaticalRelations.DETERMINER);
        excludeRels.add(EnglishGrammaticalRelations.PREDETERMINER);
        excludeRels.add(EnglishGrammaticalRelations.NUMERIC_MODIFIER);
        excludeRels.add(EnglishGrammaticalRelations.NUMBER_MODIFIER);
        excludeRels.add(EnglishGrammaticalRelations.POSSESSION_MODIFIER);
        excludeRels.add(EnglishGrammaticalRelations.POSSESSIVE_MODIFIER);
        excludeRels.add(EnglishGrammaticalRelations.QUANTIFIER_MODIFIER);
        excludeRels.add(EnglishGrammaticalRelations.TEMPORAL_MODIFIER);
        excludeRels.add(EnglishGrammaticalRelations.NP_ADVERBIAL_MODIFIER);
        excludeRels.add(EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER);
        //excludeRels.add(EnglishGrammaticalRelations.AUX_MODIFIER);
        for (IndexedWord w: object.getWordList()) {
            // Skip the words that were included afterwards (not part of the DP)
            if (w.index() < 0)
                continue;
            
            // Get the relevant modifiers to be dropped (their modifiers as well)
            Set<IndexedWord> modifiers = sg.getChildrenWithRelns(w, excludeRels);
            for (IndexedWord m: modifiers) {
                ObjectArrayList<IndexedWord> subModifiers = CoreNLPUtils.getSubTreeSortedNodes(m, sg, null);
                //if (!sm.tag().equals(POS_TAG.IN))
                dropWords.addAll(subModifiers);
            }
            dropWords.addAll(modifiers);
            
            // Drop quantities
            if (w.ner().equals(Quantity.ST_QUANTITY)) {
                dropWords.add(w);
            }
        }
        object.getWordList().removeAll(dropWords);
        // add words to dropped word list
        object.addDroppedWords(dropWords);
        dropWords.clear();
        
        // If [IN|TO] .* [IN|TO] => drop [IN|TO] .*, i.e. -> drop PP attachments
        TokenSequencePattern tPattern = TokenSequencePattern.compile(REGEX.T_PREP_ALL_PREP);
        TokenSequenceMatcher tMatcher = tPattern.getMatcher(object.getWordCoreLabelList());
        ObjectArrayList<IndexedWord> matchedWords;
        while (tMatcher.find()){
            matchedWords = WordCollectionUtils.toIndexedWordList(tMatcher.groupNodes());
            for (int i = 0; i < matchedWords.size(); i++) {
                if (matchedWords.get(i).tag().equals(POS_TAG.IN) || matchedWords.get(i).tag().equals(POS_TAG.TO)) {
                    if (i == 0) {
                        if (matchedWords.get(i).tag().equals(POS_TAG.TO) && WordUtils.isVerb(matchedWords.get(i+1)))
                            break;
                        dropWords.add(matchedWords.get(i));
                    } else break;
                } else {
                    dropWords.add(matchedWords.get(i));
                }
            }
        }
        object.getWordList().removeAll(dropWords);
        // add words to dropped word list
        object.addDroppedWords(dropWords);
        dropWords.clear();

        // TODO: if QUANT + NP + IN => drop "QUANT + NP" ?
        
        // If VB_1+ TO VB_2 => drop VB_1+ TO .*
        tPattern = TokenSequencePattern.compile(REGEX.T_VB_TO_VB);
        tMatcher = tPattern.getMatcher(object.getWordCoreLabelList());
        while (tMatcher.find()){
            matchedWords = WordCollectionUtils.toIndexedWordList(tMatcher.groupNodes());
            for (IndexedWord matchedWord : matchedWords) {
                if (matchedWord.tag().equals(POS_TAG.TO)) {
                    dropWords.add(matchedWord);
                    break;
                } else {
                    dropWords.add(matchedWord);
                }
            }
        }
        object.getWordList().removeAll(dropWords);
        // add words to dropped word list
        object.addDroppedWords(dropWords);
        dropWords.clear();
        
        // Drop auxilaries
        for (IndexedWord w: object.getWordList()) {
            if (w.index() < 0)
                continue;
            Set<IndexedWord> modifiers = sg.getChildrenWithReln(w, EnglishGrammaticalRelations.AUX_MODIFIER);
            for (IndexedWord m: modifiers) {
                ObjectArrayList<IndexedWord> subModifiers = CoreNLPUtils.getSubTreeSortedNodes(m, sg, null);
                dropWords.addAll(subModifiers);
            }
            dropWords.addAll(modifiers);
        }
        object.getWordList().removeAll(dropWords);
        // add words to dropped word list
        object.addDroppedWords(dropWords);
        dropWords.clear();
        
        // Drop noun modifiers with different NERs
        for (IndexedWord w: object.getWordList()) {
            if (w.index() < 0)
                continue;
            Set<IndexedWord> modifiers = sg.getChildrenWithReln(w, EnglishGrammaticalRelations.NOUN_COMPOUND_MODIFIER);
            for (IndexedWord mw: modifiers) {
                if (!w.ner().equals(mw.ner())) {
                    dropWords.add(mw);
                    dropWords.addAll(CoreNLPUtils.getSubTreeSortedNodes(mw, sg, null));
                }
            }
        }
        object.getWordList().removeAll(dropWords);
        // add words to dropped word list
        object.addDroppedWords(dropWords);
        dropWords.clear();
    }
}