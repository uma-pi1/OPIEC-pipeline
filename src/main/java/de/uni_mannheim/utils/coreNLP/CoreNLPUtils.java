package de.uni_mannheim.utils.coreNLP;

import java.util.List;
import java.util.Set;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphEdge;
import edu.stanford.nlp.semgraph.SemanticGraphFactory;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.BasicDependenciesAnnotation;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import edu.stanford.nlp.trees.EnglishGrammaticalStructure;
import edu.stanford.nlp.trees.TreeGraphNode;
import edu.stanford.nlp.trees.TypedDependency;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.util.Generics;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import de.uni_mannheim.constant.CHARACTER;

import de.uni_mannheim.constant.SEPARATOR;
import de.uni_mannheim.constant.WORDS;

/**
 * @author Kiril Gashteovski
 */
public class CoreNLPUtils {
    /**
     * Given a CoreNLP pipeline and an input sentence, generate dependency parse for the sentence and return
     * the SemanticGraph object as a result
     * @param pipeline - CoreNLP pipeline
     * @param snt - input sentence
     * @return dependency parse in SemanticGraph object
     */
    public static SemanticGraph parse(StanfordCoreNLP pipeline, String snt) {
        Annotation document = new Annotation(snt);
        pipeline.annotate(document);
        
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
        SemanticGraph semanticGraph = null;
        for(CoreMap sentence: sentences) {
            semanticGraph = sentence.get(BasicDependenciesAnnotation.class);
        }
        
        return semanticGraphUniversalEnglishToEnglish(semanticGraph);
    }

    /**
     * Given a list of indexed words and a semantic graph, return the head word of the word list. We assume that
     * all the words from the list can be found in the semantic graph sg, and the words in wordList are connected
     * within the semantic graph of the sentence - sg, and that they all share a common root.
     * @param wordList: the phrase from the sentence, represented as a list of words
     * @param sg: semantic graph of the sentence
     * @return the head word from the phrase
     */
    public static IndexedWord getHeadWord(ObjectArrayList<IndexedWord> wordList, SemanticGraph sg){
        // If the word list is consisted of one word - return that word
        if (wordList.size() == 1) return wordList.get(0);

        // For relations "is in"
        if (wordList.size() == 2) {
            if (wordList.get(0).index() == -2 && wordList.get(1).index() == -2) {
                return wordList.get(0);
            }
        }

        IndexedWord constituentRoot = null;
        
        // We only search as high as grandparents
        // constituentRoot = sg.getCommonAncestor(wordList.get(0), wordList.get(wordList.size()-1));

        // If the common ancestor is deeper in the tree, the constituent root is the word with shortest distance
        // to the root of the sentence
        int minPathToRoot = Integer.MAX_VALUE;
        int pathToRoot;
        for (IndexedWord w : wordList) {
            // The words with index -2 are the ones that cannot be found in the semantic graph (synthetic words)
            // This happens in the relations (see in clausie.ClauseDetector.java), and those words are the head words
            if (w.index() == -2) {
                constituentRoot = w;
                continue;
            }
            pathToRoot = sg.getShortestDirectedPathNodes(sg.getFirstRoot(), w).size();
            if (pathToRoot < minPathToRoot) {
                minPathToRoot = pathToRoot;
                constituentRoot = w;
            }
        }

        return constituentRoot;
    }
    
    /**
     * Given a list of core maps (each core map beeing a word) and a semantic graph of the sentence, return the 
     * root word of the word list (i.e. the one which is closest to the root of the semantic graph). We assume that
     * all the words from the list can be found in the semantic graph sg, and the words in wordList are connected
     * within the semantic graph of the sentence - sg, and that they all share a common root.
     * 
     * @param sg: semantic graph of the sentence
     * @param wordList: the phrase from the sentence, represented as a list of words
     * @return the root word from the phrase
     */
    public static IndexedWord getRootFromCoreMapWordList(SemanticGraph sg, List<CoreMap> wordList){
        ObjectArrayList<IndexedWord> indWordList = WordCollectionUtils.toIndexedWordList(wordList);
        return getHeadWord(indWordList, sg);
    }
    
    /**
     * Given the sentence semantic graph and a list of words, get a subgraph containing just the words in the list
     * 'words'. Each typed dependency has each word from the list as a governor.
     * @param sg: sentence semantic graph
     * @param words: list of words which should contain the semantic graph
     * @return subgraph containing the words from 'words'
     */
    public static SemanticGraph getSubgraphFromWords(SemanticGraph sg, ObjectArrayList<IndexedWord> words){        
        // Determining the root
        int minInd = Integer.MAX_VALUE;
        IndexedWord root = new IndexedWord();
        for (IndexedWord w: words){
            if (w.index() < minInd){
                minInd = w.index();
                root = w;
            }
        }
        
        // Getting the typed dependency
        ObjectArrayList<TypedDependency> tds = new ObjectArrayList<>();
        for (TypedDependency td: sg.typedDependencies()){
            if (words.contains(td.gov()) && words.contains(td.dep()))
                tds.add(td);
        }
        
        // Create the semantic graph
        TreeGraphNode rootTGN = new TreeGraphNode(new CoreLabel(root));
        EnglishGrammaticalStructure gs = new EnglishGrammaticalStructure(tds, rootTGN);

        return SemanticGraphFactory.generateUncollapsedDependencies(gs);
    }

    /**
     * Given a starting vertice, grabs the subtree encapsulated by portion of the semantic graph, excluding
     * a given edge.  A tabu list is maintained, in order to deal with cyclical relations (such as between a
     * rcmod (relative clause) and its nsubj).
     * 
     * @param vertice: starting vertice from which the sub-tree needs to be returned
     * @param sg: semantic graph of the sentence
     * @param excludedEdge: excluded edge
     * 
     * Copied from: https://github.com/stanfordnlp/CoreNLP/blob/master/src/edu/stanford/nlp/semgraph/SemanticGraphUtils.java
     */
    public static Set<SemanticGraphEdge> getSubTreeEdges(IndexedWord vertice, SemanticGraph sg, SemanticGraphEdge excludedEdge) {
        Set<SemanticGraphEdge> tabu = Generics.newHashSet();
        tabu.add(excludedEdge);
        getSubTreeEdgesHelper(vertice, sg, tabu);
        tabu.remove(excludedEdge); // Do not want this in the returned edges
        return tabu;
    }
    private static void getSubTreeEdgesHelper(IndexedWord vertice, SemanticGraph sg, Set<SemanticGraphEdge> tabuEdges) {
        for (SemanticGraphEdge edge : sg.outgoingEdgeIterable(vertice)) {
            if (!tabuEdges.contains(edge)) {
                IndexedWord dep = edge.getDependent();
                tabuEdges.add(edge);
                getSubTreeEdgesHelper(dep, sg, tabuEdges);
            }
        }
    }
    
    /**
     * Given a starting vertice, grabs the subtree encapsulated by portion of the semantic graph, excluding
     * a given edge. Returns the nodes of the subtree sorted by their indexes in the sentence.
     * 
     * @param vertice: starting vertice from which the sub-tree needs to be returned
     * @param sg: semantic graph of the sentence
     * @param excludedEdge: excluded edge
     * @return list of IndexedWord objects
     */
    public static ObjectArrayList<IndexedWord> getSubTreeSortedNodes(IndexedWord vertice, SemanticGraph sg, SemanticGraphEdge excludedEdge) {
        Set<SemanticGraphEdge> subTreeEdges = getSubTreeEdges(vertice, sg, excludedEdge);
        return WordCollectionUtils.getSortedWordsFromListOfEdges(subTreeEdges);
    }
    
    /**
     * Get the semgrex pattern for "{} < {idx:word.index()}", where we do the matching with the index of the word
     * @param word: {} is the dependent of a relation reln with 'word'
     * @return semgrex pattern string
     */
    public static String getSemgrexDependentOf(IndexedWord word){
        StringBuilder sb = new StringBuilder();
        sb.append(CHARACTER.LBRACE);
        sb.append(CHARACTER.RBRACE);
        sb.append(SEPARATOR.SPACE);
        sb.append(CHARACTER.LESS);
        sb.append(SEPARATOR.SPACE);
        sb.append(CHARACTER.LBRACE);
        sb.append(WORDS.idx);
        sb.append(CHARACTER.COLON);
        sb.append(word.index());
        sb.append(CHARACTER.RBRACE);
        return sb.toString().trim();
    }
    
    public static SemanticGraph semanticGraphUniversalEnglishToEnglish(SemanticGraph semanticGraph) {
        for (SemanticGraphEdge edge: semanticGraph.edgeListSorted()) {
            edge.setRelation(EnglishGrammaticalRelations.shortNameToGRel.get(edge.getRelation().getShortName()));
        }
        
        return semanticGraph;
    }
}