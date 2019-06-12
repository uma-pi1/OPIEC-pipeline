package avroschema.util;

import avroschema.linked.SentenceLinked;
import avroschema.linked.TokenLinked;
import avroschema.linked.TripleLinked;
import avroschema.linked.WikiLink;

import de.uni_mannheim.constant.NE_TYPE;
import de.uni_mannheim.minie.annotation.Quantity;
import de.uni_mannheim.minie.annotation.factuality.Modality;
import de.uni_mannheim.minie.annotation.factuality.Polarity;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphFactory;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.trees.EnglishGrammaticalStructure;
import edu.stanford.nlp.trees.TypedDependency;
import edu.stanford.nlp.trees.TreeGraphNode;


import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import org.apache.spark.broadcast.Broadcast;

import java.io.IOException;
import java.util.*;

/**
 * @author Kiril Gashteovski
 */
public class Utils {
    public static String sentenceLinkedToString(SentenceLinked s) {
        StringBuilder sb = new StringBuilder();
        for (TokenLinked t: s.getTokens()) {
            sb.append(t.getWord());
            sb.append(" ");
        }
        return sb.toString().trim();
    }

    public static SemanticGraph sentenceToSemanticGraph(SentenceLinked s) throws IOException {
        // Get the dependencies
        String dp = s.getDp().substring(1, s.getDp().length() -1);
        String[] dependencies = dp.split("\\), ");
        List<TokenLinked> tokens = s.getTokens();
        List<TypedDependency> tds = new ArrayList<>();
        TreeGraphNode root = null;
        for (int i = 0; i < dependencies.length; i++){
            // Each dependency is represented in the format "typed_dependency(gov-ind, dep-ind"
            String dep = dependencies[i];
            if (i == dependencies.length - 1){
                dep = dep.substring(0, dep.length()-1);
            }
            try {
                // Separate the typed dependency from the head word and the dependent
                String[] tDep = dep.split("\\(");
                String typedDep = tDep[0];
                String gov = tDep[1].split(", ")[0];
                String dependent = tDep[1].split(", ")[1];

                // IndexedWord object for the head word
                IndexedWord iGov = new IndexedWord();
                String [] govSplit = gov.split("-");
                int govIndex = Integer.parseInt(govSplit[govSplit.length-1]);

                // IndexedWord object for the dependent word
                IndexedWord iDep = new IndexedWord();
                String [] depSplit = dependent.split("-");
                int depIndex = Integer.parseInt(depSplit[depSplit.length - 1]);

                // Set the governor and dependent IndexedWord
                iDep.setWord(tokens.get(depIndex - 1).getWord());
                iDep.setOriginalText(tokens.get(depIndex - 1).getWord());
                iDep.setTag(tokens.get(depIndex - 1).getPos());
                iDep.setNER(tokens.get(depIndex - 1).getNer());
                iDep.setIndex(depIndex);
                iDep.setValue(tokens.get(depIndex - 1).getWord());
                iDep.setLemma(tokens.get(depIndex - 1).getLemma());

                if (govIndex == 0){
                    iGov.setIndex(govIndex);
                    iGov.setWord("ROOT");
                    iGov.setValue("ROOT");
                    root = new TreeGraphNode(new CoreLabel(iDep));
                } else {
                    iGov.setIndex(govIndex);
                    iGov.setWord(tokens.get(govIndex-1).getWord());
                    iGov.setTag(tokens.get(govIndex-1).getPos());
                    iGov.setNER(tokens.get(govIndex-1).getNer());
                    iGov.setOriginalText(tokens.get(govIndex-1).getWord());
                    iGov.setValue(tokens.get(govIndex-1).getWord());
                    iGov.setLemma(tokens.get(govIndex-1).getLemma());
                }

                // Setting the english grammatical relation between the words
                GrammaticalRelation r = EnglishGrammaticalRelations.shortNameToGRel.get(typedDep);
                if (typedDep.equals("root")){
                    //continue;
                    r = GrammaticalRelation.ROOT;
                    //continue;
                }
                // Add the typed dependency to the list
                tds.add(new TypedDependency(r, iGov, iDep));
            } catch(IndexOutOfBoundsException | NumberFormatException e) {
                return new SemanticGraph();
            }
        }

        EnglishGrammaticalStructure gs = new EnglishGrammaticalStructure(tds, root);

        //SemanticGraphFactory.generateUncollapsedDependencies(sg);
        return SemanticGraphFactory.generateUncollapsedDependencies(gs);
    }

    /**
     * Given a list of tokens, transform them into a string of words or lemmas and check if they are in a dictionary.
     * @param tokens list of tokens
     * @param dict dictionary of "concepts" (broadcasted variable)
     * @return true if tokens are found in dictionary
     */
    public static boolean isInDict(List<TokenLinked> tokens, Broadcast<ObjectOpenHashSet<String>> dict) {
        if (tokens.size() == 0) {
            return false;
        }

        String tokensLemmatized = TokenLinkedUtils.linkedTokensLemmasToString(tokens).toLowerCase();
        String tokensWords = TokenLinkedUtils.linkedTokensWordsToString(tokens).toLowerCase();

        if (dict.value().contains(tokensWords)) {
            return true;
        }
        else if (dict.value().contains(tokensLemmatized)) {
            return true;
        }

        return false;
    }

    /**
     * Given a list of linked token and a dictionary of terms, check if:
     *  a) the tokens are a QUANTITY modifying a term in a dictionary, or
     *  b) the tokens are a term in a dictionary modified by a QUANTITY
     * @param tokens: list of linked tokens
     * @param dict: dictionary of terms (broadcasted variable)
     * @return true if a) or b), false otherwise
     */
    public static boolean isInDictWithQuantity(List<TokenLinked> tokens, Broadcast<ObjectOpenHashSet<String>> dict) {
        if (tokens.size() <= 1) {
            return false;
        }

        boolean isFirstTokenQuantity = tokens.get(0).getNer().equals(NE_TYPE.QUANTITY);
        boolean isLastTokenQuantity = tokens.get(tokens.size() - 1).getNer().equals(NE_TYPE.QUANTITY);

        if (!isFirstTokenQuantity && !isLastTokenQuantity) {
            return false;
        }

        // Check if the tokens are "QUANTITY T1 T2 ... Tn"
        List<TokenLinked> sublist = tokens.subList(1, tokens.size());
        boolean isSubListInDict = dict.value().contains(TokenLinkedUtils.linkedTokensWordsToString(sublist).toLowerCase()) ||
                                  dict.value().contains(TokenLinkedUtils.linkedTokensLemmasToString(sublist).toLowerCase());
        if (isFirstTokenQuantity && isSubListInDict) {
            return true;
        }

        // Check if the tokens are "T1 T2 ... Tn QUANTITY"
        sublist = tokens.subList(0, tokens.size() - 1);
        isSubListInDict = dict.value().contains(TokenLinkedUtils.linkedTokensWordsToString(sublist).toLowerCase()) ||
                          dict.value().contains(TokenLinkedUtils.linkedTokensLemmasToString(sublist).toLowerCase());
        if (isLastTokenQuantity && isSubListInDict) {
            return true;
        }

        return false;
    }

    /**
     * Given a list of sentences and a list of links containing information about the links within these sentences,
     * annotate each token with its link (if found)
     * @param sentences list of linked sentences
     * @param links list of links contained within the sentences
     * @return the same list of sentences, only annotated with links
     */
    public static List<SentenceLinked> getSentencesWithLinkedTokens(List<SentenceLinked> sentences, List<WikiLink> links) {
        Set<Integer> beginOffsetsForLinks = new HashSet<>();

        for (WikiLink wl: links) {
            beginOffsetsForLinks.add(wl.getOffsetBeginInd());
        }

        for (SentenceLinked s: sentences) {
            for (int i = 0; i < s.getTokens().size(); i++) {
                TokenLinked t = s.getTokens().get(i);
                int tGlobalStartIndex = s.getSpan().getStartIndex() + t.getSpan().getStartIndex();
                int tGlobalEndIndex = s.getSpan().getStartIndex() + t.getSpan().getEndIndex();
                if (beginOffsetsForLinks.contains(tGlobalStartIndex)) {
                    for (WikiLink wl: links) {
                        if (tGlobalStartIndex == wl.getOffsetBeginInd()) {
                            t.setWLink(wl);
                            while (tGlobalEndIndex < wl.getOffsetEndInd()) {
                                i++;

                                // This happens when sentence splitter splits the sentence within the link (rare)
                                if (i == s.getTokens().size()) {
                                    break;
                                }

                                t = s.getTokens().get(i);
                                t.setWLink(wl);
                                tGlobalEndIndex = s.getSpan().getStartIndex() + t.getSpan().getEndIndex();
                            }
                        }
                    }
                }
            }
        }

        return sentences;
    }

    /**
     * @param triple: linked triple
     * @return true -> if the triple contains sem. annotation; false -> otherwise
     */
    public static boolean hasSemanticAnnotation(TripleLinked triple) {
        if (triple.getPolarity().equals(Polarity.ST_NEGATIVE)) {
            return true;
        }
        if (triple.getModality().equals(Modality.ST_POSSIBILITY)) {
            return true;
        }
        if (triple.getAttributionLinked() != null) {
            return true;
        }
        if (hasQuantity(triple)) {
            return true;
        }
        if (hasTemporalAnnotation(triple)) {
            return true;
        }
        if (hasSpatialAnnotation(triple)) {
            return true;
        }
        return false;
    }

    /**
     * @param triple: linked triple
     * @return true -> if the triple contains quantity; false -> otherwise
     */
    public static boolean hasQuantity(TripleLinked triple) {
        for (TokenLinked t: triple.getSubject()) {
            if (t.getNer().equals(Quantity.ST_QUANTITY)) {
                return true;
            }
        }
        for (TokenLinked t: triple.getRelation()) {
            if (t.getNer().equals(Quantity.ST_QUANTITY)) {
                return true;
            }
        }
        for (TokenLinked t: triple.getObject()) {
            if (t.getNer().equals(Quantity.ST_QUANTITY)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param triple linked triple
     * @return true -> if the triple contains temporal annotation(s); false -> otherwise
     */
    public static boolean hasTemporalAnnotation(TripleLinked triple) {
        if (triple.getTimeLinked() != null) {
            return true;
        }
        if (triple.getSubjTimeLinked().getTimeLinked().getAllWords().size() > 0) {
            return true;
        }
        if (triple.getRelTimeLinked().getTimeLinked().getAllWords().size() > 0) {
            return true;
        }
        if (triple.getObjTimeLinked().getTimeLinked().getAllWords().size() > 0) {
            return true;
        }
        for (TokenLinked token: triple.getSubject()) {
            if (TokenLinkedUtils.tokenIsTemporal(token)) {
                return true;
            }
        }
        for (TokenLinked token: triple.getRelation()) {
            if (TokenLinkedUtils.tokenIsTemporal(token)) {
                return true;
            }
        }
        for (TokenLinked token: triple.getObject()) {
            if (TokenLinkedUtils.tokenIsTemporal(token)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param triple linked triple
     * @return true -> if the triple contains spatial annotation(s); false -> otherwise
     */
    public static boolean hasSpatialAnnotation(TripleLinked triple) {
        if (triple.getSpaceLinked() != null) {
            return true;
        }
        if (triple.getSubjSpaceLinked().getSpace().getAllWords().size() > 0) {
            return true;
        }
        if (triple.getRelSpaceLinked().getSpace().getAllWords().size() > 0) {
            return true;
        }
        if (triple.getObjSpaceLinked().getSpace().getAllWords().size() > 0) {
            return true;
        }
        for (TokenLinked token: triple.getSubject()) {
            if (TokenLinkedUtils.tokenIsSpatial(token)) {
                return true;
            }
        }
        for (TokenLinked token: triple.getRelation()) {
            if (TokenLinkedUtils.tokenIsSpatial(token)) {
                return true;
            }
        }
        for (TokenLinked token: triple.getObject()) {
            if (TokenLinkedUtils.tokenIsSpatial(token)) {
                return true;
            }
        }
        return false;
    }
}
