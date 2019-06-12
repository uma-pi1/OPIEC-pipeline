package de.uni_mannheim.utils.coreNLP;

import de.uni_mannheim.constant.NE_TYPE;
import de.uni_mannheim.constant.POS_TAG;
import de.uni_mannheim.constant.SEPARATOR;
import de.uni_mannheim.utils.fastutils.FastUtil;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.IndexedWord;

import edu.stanford.nlp.semgraph.SemanticGraphEdge;
import edu.stanford.nlp.util.CoreMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.List;
import java.util.Set;

/**
 * @author Kiril Gashteovski
 */
public class WordCollectionUtils {
    /**
     * Given a pivot word and a list of words, return a list of "chained words" (i.e. words with same tags, or NERs
     * to the left and right of the pivot word in the list).
     *
     * @param pivot: the pivot word being examined
     * @param words: list of words from which the pivot word is part of
     * @return a list of "chained words"
     */
    public static ObjectArrayList<IndexedWord> getChainedWords(IndexedWord pivot, ObjectArrayList<IndexedWord> words){
        // TODO: double check how we generate chained words (considering the NERs)
        // In case the pivot word is not in the list - return empty list
        if (words.indexOf(pivot) == -1)
            return new ObjectArrayList<>();

        if (!pivot.ner().equals(NE_TYPE.NO_NER)) {
            return getChainedNERs(words, words.indexOf(pivot));
        } else if (WordUtils.isNoun(pivot)) {
            return getChainedNouns(words, words.indexOf(pivot));
        }
        else {
            return getChainedTagNoNER(words, words.indexOf(pivot));
        }
    }

    /**
     * Given a sequence of words and a pivot-word index, return the chained nouns from the left and from the right
     * of the pivot word.
     *
     * @param sequence: a sequence of words (list of IndexedWord)
     * @param wordInd: the index of the pivot word
     * @return a list of chained nouns to the left and the right of the pivot word (the pivot word is included)
     */
    public static ObjectArrayList<IndexedWord> getChainedNouns(ObjectArrayList<IndexedWord> sequence, int wordInd){
        IntArrayList chainedNounsInd = new IntArrayList();

        // Get the chained nouns from left and right
        IntArrayList chainedNounsLeft = getChainedNounsFromLeft(sequence, chainedNounsInd.clone(), wordInd);
        IntArrayList chainedNounsRight = getChainedNounsFromRight(sequence, chainedNounsInd.clone(), wordInd);

        // Add all the words to the chained nouns
        chainedNounsInd.addAll(chainedNounsLeft);
        chainedNounsInd.add(wordInd);
        chainedNounsInd.addAll(chainedNounsRight);

        // IndexedWord chained nouns
        ObjectArrayList<IndexedWord> iChainedNouns = new ObjectArrayList<>();
        for (int i: FastUtil.sort(chainedNounsInd)){
            iChainedNouns.add(sequence.get(i));
        }

        return iChainedNouns;
    }

    /**
     * Given a sequence of indexed words and a noun, get all the nouns 'chained' to the word from the left.
     * @param sequence: a list of words
     * @param wordInd: the word index from where the search starts
     * @return a list of nouns which precede 'word'
     */
    private static IntArrayList getChainedNounsFromLeft(ObjectArrayList<IndexedWord> sequence,
                                                        IntArrayList chainedNouns, int wordInd){
        // If the word is the leftiest word or it's not a noun - return
        if (wordInd > 0 && WordUtils.isNoun(sequence.get(wordInd-1))){
            chainedNouns.add(wordInd-1);
            getChainedNounsFromLeft(sequence, chainedNouns, wordInd-1);
        }

        return chainedNouns;
    }


    /**
     * Given a sequence of indexed words and a noun, get all the nouns 'chained' to the word from the right.
     * @param sequence: a list of words
     * @param wordInd: the word index from where the search starts
     * @return a list of nouns which precede 'word'
     */
    private static IntArrayList getChainedNounsFromRight(ObjectArrayList<IndexedWord> sequence,
                                                         IntArrayList chainedNouns, int wordInd){
        // If the word is the rightiest word or it's not a noun - return
        if (wordInd < sequence.size()-1 && WordUtils.isNoun(sequence.get(wordInd+1))){
            chainedNouns.add(wordInd + 1);
            getChainedNounsFromRight(sequence, chainedNouns, wordInd + 1);
        }

        return chainedNouns;
    }

    /**
     * Given a sequence of words and a pivot-word index, return the "chained words" from the left and from the right
     * of the pivot word. "Chained words" are a list of words, which all of them share the same POS tag and have no
     * NE types.
     *
     * @param sequence: a sequence of words (list of IndexedWord)
     * @param wordInd: the index of the pivot word
     * @return a list of chained words to the left and the right of the pivot word (the pivot word is included)
     */
    public static ObjectArrayList<IndexedWord> getChainedTagNoNER(ObjectArrayList<IndexedWord> sequence, int wordInd){
        IntArrayList chainedPosWordsInd = new IntArrayList();

        // Get the chained nouns from left and right
        IntArrayList chainedPosWordsLeft = getChainedTagsFromLeftNoNER(sequence, chainedPosWordsInd.clone(), wordInd);
        IntArrayList chainedPosWordsRight = getChainedTagsFromRightNoNER(sequence, chainedPosWordsInd.clone(), wordInd);

        // Add all the words to the chained nouns
        chainedPosWordsInd.addAll(chainedPosWordsLeft);
        chainedPosWordsInd.add(wordInd);
        chainedPosWordsInd.addAll(chainedPosWordsRight);

        // IndexedWord chained nouns
        ObjectArrayList<IndexedWord> iChainedNouns = new ObjectArrayList<>();
        for (int i: FastUtil.sort(chainedPosWordsInd)){
            iChainedNouns.add(sequence.get(i));
        }

        return iChainedNouns;
    }

    /**
     * Given a sequence of indexed words and a pivot, get all the words 'chained' to the word from the left (i.e. having
     * the same POS tag as the pivot word). Also, the chained words should not have NE types.
     *
     * @param sequence: a list of words
     * @param wordInd: the word index from where the search starts
     * @return a list of words which precede 'word'
     */
    private static IntArrayList getChainedTagsFromLeftNoNER(ObjectArrayList<IndexedWord> sequence,
                                                            IntArrayList chainedPosWords, int wordInd){
        // If the word is the leftiest word or it's not with the same POS tag - return
        if (wordInd > 0 && sequence.get(wordInd).tag().equals(sequence.get(wordInd-1).tag()) &&
                sequence.get(wordInd-1).ner().equals(NE_TYPE.NO_NER)){
            chainedPosWords.add(wordInd-1);
            getChainedTagsFromLeftNoNER(sequence, chainedPosWords, wordInd-1);
        }

        return chainedPosWords;
    }

    /**
     * Given a sequence of indexed words and a noun, get all the nouns 'chained' to the word from the right.
     * Also, the chained nouns should not have NE types.
     * @param sequence: a list of words
     * @param wordInd: the word index from where the search starts
     * @return a list of nouns which preced 'word'
     */
    private static IntArrayList getChainedTagsFromRightNoNER(ObjectArrayList<IndexedWord> sequence,
                                                             IntArrayList chainedNouns, int wordInd){
        // If the word is the rightiest word or it's not a noun - return
        if (wordInd < sequence.size()-1 && sequence.get(wordInd).tag().equals(sequence.get(wordInd+1).tag()) &&
                sequence.get(wordInd+1).ner().equals(NE_TYPE.NO_NER)){
            chainedNouns.add(wordInd + 1);
            getChainedTagsFromRightNoNER(sequence, chainedNouns, wordInd + 1);
        }

        return chainedNouns;
    }

    /**
     * Given a sequence of words and a pivot-word index, return the chained words of same NER, both from the left and
     * from the right of the pivot word (it is assumed that the pivot word is also NER).
     * @param sequence: a sequence of words (list of IndexedWord)
     * @param wordInd: the index of the pivot word
     * @return a list of chained nouns to the left and the right of the pivot word (the pivot word is included)
     */
    public static ObjectArrayList<IndexedWord> getChainedNERs(ObjectArrayList<IndexedWord> sequence, int wordInd){
        IntArrayList chainedNounsInd = new IntArrayList();

        // Get the chained nouns from left and right
        IntArrayList chainedNounsLeft = getChainedNERsFromLeft(sequence, chainedNounsInd.clone(), wordInd,
                sequence.get(wordInd).ner());
        IntArrayList chainedNounsRight = getChainedNERsFromRight(sequence, chainedNounsInd.clone(), wordInd,
                sequence.get(wordInd).ner());

        // Add all the words to the chained nouns
        chainedNounsInd.addAll(chainedNounsLeft);
        chainedNounsInd.add(wordInd);
        chainedNounsInd.addAll(chainedNounsRight);

        // IndexedWord chained nouns
        ObjectArrayList<IndexedWord> iChainedNouns = new ObjectArrayList<>();
        for (int i: FastUtil.sort(chainedNounsInd)){
            iChainedNouns.add(sequence.get(i));
        }

        return iChainedNouns;
    }

    /**
     * Given a sequence of indexed words and a NER word, get all the NERs 'chained' to the word from the left (they all
     * must have the same NER).
     * @param sequence: a list of words
     * @param wordInd: the word index from where the search starts (the pivot word)
     * @param ner: the NE type of the pivot word
     * @return a list of nouns which preced 'word'
     */
    private static IntArrayList getChainedNERsFromLeft(ObjectArrayList<IndexedWord> sequence,
                                                       IntArrayList chainedNERs, int wordInd, String ner){
        // If the word is the leftiest word or it's not a noun - return
        if (wordInd > 0 && sequence.get(wordInd-1).ner().equals(ner)){
            chainedNERs.add(wordInd-1);
            getChainedNERsFromLeft(sequence, chainedNERs, wordInd-1, ner);
        }

        return chainedNERs;
    }

    /**
     * Given a sequence of indexed words and a NER word, get all the NERs 'chained' to the word from the right (they all
     * must have the same NER).
     * @param sequence: a list of words
     * @param wordInd: the word index from where the search starts (the pivot word)
     * @param ner: the NE type of the pivot word
     * @return a list of nouns which preced 'word'
     */
    private static IntArrayList getChainedNERsFromRight(ObjectArrayList<IndexedWord> sequence,
                                                        IntArrayList chainedNERs, int wordInd, String ner){
        // If the word is the rightiest word or it's not a noun - return
        if (wordInd < sequence.size()-1 && sequence.get(wordInd+1).ner().equals(ner)){
            chainedNERs.add(wordInd + 1);
            getChainedNERsFromRight(sequence, chainedNERs, wordInd + 1, ner);
        }

        return chainedNERs;
    }

    /**
     * Given a list of words (as core maps), return the phrase of words as a whole string, separated with empty space
     * @param cmList: list of words (e.g. [She, is, pretty])
     * @return string of the list of words separated by space (e.g. it returns "She is pretty")
     */
    public static String toWordString(List<CoreMap> cmList){
        StringBuilder sbSentence = new StringBuilder();
        CoreLabel cl;
        for (CoreMap cm: cmList){
            cl = new CoreLabel(cm);
            sbSentence.append(cl.word().toLowerCase());
            sbSentence.append(SEPARATOR.SPACE);
        }
        return sbSentence.toString().trim();
    }

    /**
     * Given a list of words (as core maps), return the phrase of words as a list of indexed word objects
     * @param cmList: list of words (e.g. [She, is, pretty])
     * @return list of words (as IndexedWord)
     */
    public static ObjectArrayList<IndexedWord> toIndexedWordList(List<CoreMap> cmList){
        ObjectArrayList<IndexedWord> wordList = new ObjectArrayList<>();
        for (CoreMap cm: cmList){
            wordList.add(new IndexedWord(new CoreLabel(cm)));
        }
        return wordList;
    }

    /**
     * Given a list of words, return the phrase of words' lemmas as a whole string, separated with empty space
     * @param words: list of words (e.g. [She, is, pretty])
     * @return string of the list of words separated by space (e.g. it returns "She be pretty")
     */
    public static String toLemmaString(ObjectArrayList<IndexedWord> words){
        StringBuilder sbSentence = new StringBuilder();
        for (IndexedWord word : words) {
            sbSentence.append(word.lemma());
            sbSentence.append(SEPARATOR.SPACE);
        }
        return sbSentence.toString().trim();
    }

    /**
     * Given a list of words (as core maps), return the phrase of words' lemmas as a whole string, separated with empty space
     * @param cmList: list of words (e.g. [She, is, pretty])
     * @return string of the list of words separated by space (e.g. it returns "She be pretty")
     */
    public static String toLemmaString(List<CoreMap> cmList){
        StringBuilder sbSentence = new StringBuilder();
        CoreLabel cl;
        for (CoreMap cm: cmList){
            cl = new CoreLabel(cm);
            sbSentence.append(cl.lemma().toLowerCase());
            sbSentence.append(SEPARATOR.SPACE);
        }
        return sbSentence.toString().trim();
    }

    /**
     * Given a list of indexed words, sort them by sentence index, and return them as a list of indexed words
     * @param wordList: list of words to be sorted by sentence index
     * @return list of indexed words (wordSet sorted by sentence index)
     */
    public static ObjectArrayList<IndexedWord> getSortedWords(ObjectArrayList<IndexedWord> wordList){
        ObjectArrayList<IndexedWord> sortedWords = new ObjectArrayList<>();
        IntArrayList wordsIndices = new IntArrayList();
        for (IndexedWord w: wordList){
            wordsIndices.add(w.index());
        }
        int [] sorted = FastUtil.sort(wordsIndices);
        for (int x: sorted){
            for (IndexedWord w: wordList){
                if (w.index() == x){
                    sortedWords.add(w);
                }
            }
        }

        return sortedWords;
    }

    /**
     * Get the number of prepositions in the list of words (TO is also counted)
     * @param wList: list of words
     * @return number of prepositions in the list
     */
    public static int countPrepositions(ObjectArrayList<IndexedWord> wList){
        int prepCount = 0;
        for (IndexedWord w: wList){
            if (w.tag().equals(POS_TAG.IN) || w.tag().equals(POS_TAG.TO))
                prepCount++;
        }
        return prepCount;
    }

    public static ObjectArrayList<CoreLabel> getCoreLabelList(ObjectArrayList<IndexedWord> words) {
        ObjectArrayList<CoreLabel> coreLabelList = new ObjectArrayList<>();
        for (IndexedWord w: words) {
            coreLabelList.add(new CoreLabel(w));
        }
        return coreLabelList;
    }

    public static ObjectArrayList<IndexedWord> getWordList(List<CoreMap> coreMapList){
        ObjectArrayList<IndexedWord> coreLabelList = new ObjectArrayList<>();
        for (CoreMap cm: coreMapList){
            coreLabelList.add(new IndexedWord(new CoreLabel(cm)));
        }
        return coreLabelList;
    }
    public static ObjectOpenHashSet<IndexedWord> getWordSet(List<CoreMap> coreMapList){
        ObjectOpenHashSet<IndexedWord> coreLabelSet = new ObjectOpenHashSet<>();
        for (CoreMap cm: coreMapList){
            coreLabelSet.add(new IndexedWord(new CoreLabel(cm)));
        }
        return coreLabelSet;
    }

    /**
     * Given a list of edges, get all the indexed words from them (their nodes) and return them sorted by index
     * @param edges: list of edges
     * @return list of indexed words sorted by index
     */
    public static ObjectArrayList<IndexedWord> getSortedWordsFromListOfEdges(Set<SemanticGraphEdge> edges){
        ObjectOpenHashSet<IndexedWord> wordsSet = new ObjectOpenHashSet<>();
        for (SemanticGraphEdge e: edges){
            wordsSet.add(e.getGovernor());
            wordsSet.add(e.getDependent());
        }

        return getSortedWords(wordsSet);
    }

    /**
     * Given a set of indexed words, sort them by sentence index, and return them as a list of indexed words
     * @param wordSet: set of words to be sorted by sentence index
     * @return list of indexed words (wordSet sorted by sentence index)
     */
    public static ObjectArrayList<IndexedWord> getSortedWords(Set<IndexedWord> wordSet){
        ObjectArrayList<IndexedWord> sortedWords = new ObjectArrayList<>();
        IntArrayList wordsIndices = new IntArrayList();
        for (IndexedWord w: wordSet){
            wordsIndices.add(w.index());
        }
        int [] sorted = FastUtil.sort(wordsIndices);
        for (int x: sorted){
            for (IndexedWord w: wordSet){
                if (w.index() == x){
                    sortedWords.add(w);
                }
            }
        }

        return sortedWords;
    }

    /**
     * Given a fast util object list of indexed words, return object array list of the same object list
     * @param oWordList: list of indexed word (object list)
     * @return an object array list object of oWordList
     */
    public static ObjectArrayList<IndexedWord> toObjectArrayList(ObjectList<IndexedWord> oWordList){
        ObjectArrayList<IndexedWord> oaWordList = new ObjectArrayList<>();
        oaWordList.addAll(oWordList);
        return oaWordList.clone();
    }

    /**
     * Given a sequence of indexed words, return a string in the format "[POS1|NER1] [POS2|NER2] ... [POSn|NERn]"
     * If a given word has a NER type -> write the type, else -> write the POS tag.
     * When we have a verb, noun, adverb,...unify them under a "common" POS tag (e.g:VB for all verbs, NN for all nouns,etc.)
     * @param words: a list of indexed words
     * @return a string in the format "[POS1|NER1] [POS2|NER2] ... [POSn|NERn]"
     */
    public static String wordsToPosMergedNerSeq(ObjectArrayList<IndexedWord> words){
        StringBuilder sbSeq = new StringBuilder();
        for (IndexedWord word : words) {
            if (word.ner().equals(NE_TYPE.NO_NER)) {
                if (WordUtils.isAdj(word))
                    sbSeq.append(POS_TAG.JJ);
                else if (WordUtils.isAdverb(word))
                    sbSeq.append(POS_TAG.RB);
                else if (WordUtils.isNoun(word))
                    sbSeq.append(POS_TAG.NN);
                else if (WordUtils.isPronoun(word))
                    sbSeq.append(POS_TAG.PR);
                else if (WordUtils.isVerb(word))
                    sbSeq.append(POS_TAG.VB);
                else if (WordUtils.isWhPronoun(word))
                    sbSeq.append(POS_TAG.WP);
                else sbSeq.append(word.tag());

                sbSeq.append(SEPARATOR.SPACE);
            } else {
                sbSeq.append(word.ner());
                sbSeq.append(SEPARATOR.SPACE);
            }
        }
        return sbSeq.toString().trim();
    }

    /**
     * Given a list of words, check if there is a verb in the list
     * @param words: list of indexed words
     * @return true -> if there is a verb in the list of words, false -> otherwise
     */
    public static boolean hasVerb(ObjectArrayList<IndexedWord> words){
        for (IndexedWord word: words){
            if (WordUtils.isVerb(word)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Given a list of IndexedWord objects, return a list consisted of the indices of the words within the sentence
     * (from the words of the list)
     * @param wordList list of words
     * @return list of indices of the words in wordList within the sentence
     */
    public static IntArrayList getWordIndices(ObjectArrayList<IndexedWord> wordList) {
        IntArrayList indices = new IntArrayList();
        for (IndexedWord w: wordList) {
            indices.add(w.index());
        }
        return indices;
    }


    public static boolean isOneNER(ObjectArrayList<IndexedWord> wordList) {
        String firstType = wordList.get(0).ner();
        if (firstType.equals(NE_TYPE.NO_NER)) {
            return false;
        }
        boolean isOneNER = true;
        for (IndexedWord w: wordList) {
            if (!w.ner().equals(firstType)) {
                isOneNER = false;
            }
        }
        return isOneNER;
    }

    public static boolean isLocation(ObjectArrayList<IndexedWord> wordList) {
        boolean check = true;
        for (IndexedWord w: wordList) {
            if (!w.ner().equals(NE_TYPE.LOCATION)) {
                check = false;
                break;
            }
        }
        return check;
    }
}
