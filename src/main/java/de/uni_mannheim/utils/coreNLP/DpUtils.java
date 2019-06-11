package de.uni_mannheim.utils.coreNLP;

import java.util.*;

import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphEdge;

/** This class provides a set of utilities to work with {@link SemanticGraph}
 * For details on the Dependency parser @see <a href="nlp.stanford.edu/software/dependencies_manual.pdf">the Stanford Parser manual  
 *
 * @author Luciano del Corro
 * @author Kiril Gashteovski
 */
public class DpUtils {
    
    private static final int MAX_RECURSION_ITERATIONS = 500;
    
	/** Finds the first occurrence of a grammatical relation in a set of edges */
    public static SemanticGraphEdge findFirstOfRelation(List<SemanticGraphEdge> edges, GrammaticalRelation rel) {
        for (SemanticGraphEdge e : edges) {
            if (rel.equals(e.getRelation())) {
                return e;
            }
        }
        return null;
    }

    /** Finds the first occurrence of a grammatical relation or its descendants in a set of edges */
    public static SemanticGraphEdge findFirstOfRelationOrDescendent(List<SemanticGraphEdge> edges, GrammaticalRelation rel) {
        for (SemanticGraphEdge e : edges) {
            if (rel.isAncestor(e.getRelation())) {
                return e;
            }
        }
        return null;
    }

    /** Finds the first occurrence of a grammatical relation or its descendants for a relative pronoun */
    public static SemanticGraphEdge findDescendantRelativeRelation(SemanticGraph semanticGraph, IndexedWord root, 
            GrammaticalRelation rel) {
        List<SemanticGraphEdge> outedges = semanticGraph.getOutEdgesSorted(root);
        for (SemanticGraphEdge e: outedges) {
            if (e.getDependent().tag().charAt(0) == 'W') {
                if (rel.isAncestor(e.getRelation())) {
                    return e;
                } else {
                    return findDescendantRelativeRelation(semanticGraph, e.getDependent(), rel);
                }
            } else {
                return findDescendantRelativeRelation(semanticGraph, e.getDependent(), rel);
            }
        }
        return null;
    }

    /** Finds all occurrences of a grammatical relation or its descendants in a set of edges */
    public static List<SemanticGraphEdge> getEdges(List<SemanticGraphEdge> edges, GrammaticalRelation rel) {
        List<SemanticGraphEdge> result = new ArrayList<>();
        for (SemanticGraphEdge e : edges) {
            if (rel.isAncestor(e.getRelation())) {
                result.add(e);
            }
        }
        return result;
    }

    /** Checks if a given edge holds a subject relation*/
    public static boolean isAnySubj(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.SUBJECT.isAncestor(edge.getRelation());
    }

    /** Checks if a given edge holds an object relation */
    public static boolean isAnyObj(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.OBJECT.isAncestor(edge.getRelation());
    }

    /** Checks if a given edge holds a prepositional object relation*/
    public static boolean isPobj(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.PREPOSITIONAL_OBJECT.equals(edge.getRelation());
    }

    /** Checks if a given edge holds a direct object relation */
    public static boolean isDobj(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.DIRECT_OBJECT.equals(edge.getRelation());
    }

    /** Checks if a given edge holds an indirect object relation */
    public static boolean isIobj(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.INDIRECT_OBJECT.equals(edge.getRelation());
    }

    /** Checks if a given edge holds a negation relation */
    public static boolean isNeg(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.NEGATION_MODIFIER.equals(edge.getRelation());
    }

    /** Checks if a given edge holds the 'dep' relation */
    public static boolean isDep(SemanticGraphEdge edge) {
        return edge.toString().equals("dep");
    }

    /** Checks if a given edge holds an apposittional relation */
    public static boolean isAppos(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.APPOSITIONAL_MODIFIER.equals(edge.getRelation());
    }

    /** Checks if a given edge holds a xcomp relation */
    public static boolean isXcomp(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.XCLAUSAL_COMPLEMENT.equals(edge.getRelation());
    }

    /** Checks if a given edge holds an expletive relation */
    public static boolean isExpl(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.EXPLETIVE.equals(edge.getRelation());
    }

    /** Checks if a given edge holds an adjectival complement relation */
    public static boolean isAcomp(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.ADJECTIVAL_COMPLEMENT.equals(edge.getRelation());
    }

    /** Checks if a given edge holds a prepositional modifier relation */
    public static boolean isAnyPrep(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER.isAncestor(edge.getRelation());
    }

    /** Checks if a given edge holds a copular relation */
    public static boolean isCop(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.COPULA.equals(edge.getRelation());
    }

    /** Checks if a given edge holds an adverbial clausal relation */
    public static boolean isAdvcl(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.ADV_CLAUSE_MODIFIER.equals(edge.getRelation());
    }

    /** Checks if a given edge holds a relative clause modifier relation */
    public static boolean isRcmod(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.RELATIVE_CLAUSE_MODIFIER.equals(edge.getRelation());
    }

    /** Checks if a given edge holds a clausal complement relation */
    public static boolean isCcomp(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.CLAUSAL_COMPLEMENT.equals(edge.getRelation());
    }

    /** Checks if a given edge holds an adverbial modifier relation */
    public static boolean isAdvmod(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.ADVERBIAL_MODIFIER.equals(edge.getRelation());
    }

    /** Checks if a given edge holds an np adverbial modifier relation */
    public static boolean isNpadvmod(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.NP_ADVERBIAL_MODIFIER.equals(edge.getRelation());
    }

    /** Checks if a given edge holds a marker relation */
    public static boolean isMark(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.MARKER.equals(edge.getRelation());
    }

    /** Checks if a given edge holds a possession modifier relation */
    public static boolean isPoss(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.POSSESSION_MODIFIER.equals(edge.getRelation());
    }

    /*
     *
        Checks if a given edge holds a participial modifier relation

      	The partmod and infmod relations were deleted, and replaced with
		vmod for reduced, non-finite verbal modifiers. The distinction between
		these two relations can be recovered from the POS tag of the dependent.
		
		A reduced non-finite verbal modifier is a participial or infinitive
		form of a verb heading a phrase (which may have some arguments,
		roughly like a VP). These are used to modify the meaning of an NP or
		another verb. They are not core arguments of a verb 
		or full finite relative clauses.
     */
    public static boolean isVerbMod(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.VERBAL_MODIFIER.equals(edge.getRelation());
    }
    

    /** Checks if a given edge holds a temporal modifier relation */
    public static boolean isTmod(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.TEMPORAL_MODIFIER.equals(edge.getRelation());
    }

    /** Checks if a given edge holds a conjunct relation */
    public static boolean isAnyConj(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.CONJUNCT.isAncestor(edge.getRelation());
    }

    /** Checks if a given edge holds a preconjunct modifier relation */
    public static boolean isPreconj(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.PRECONJUNCT.equals(edge.getRelation());
    }

    /** Checks if a given edge holds a coordination relation */
    public static boolean isCc(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.COORDINATION.equals(edge.getRelation());
    }

    /** Checks if a given edge holds an auxiliar modifier relation */
    public static boolean isAux(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.AUX_MODIFIER.equals(edge.getRelation());
    }

    /** Checks if a given edge holds a 'rel' relation */
    public static boolean isRel(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.RELATIVE.equals(edge.getRelation());
    }

    /** Checks if a given edge holds a parataxis relation */
    public static boolean isParataxis(SemanticGraphEdge edge) {
        return EnglishGrammaticalRelations.PARATAXIS.equals(edge.getRelation());
    }
    
    /** Checks if a given edge holds a predeterminer relation */
    public static boolean isPredet(SemanticGraphEdge edge) {
    	return EnglishGrammaticalRelations.PREDETERMINER.equals(edge.getRelation());
	}

    /** Removes some edges from the given semantic graph.
     * 
     * This method traverses the semantic graph starting from the given root. An edge is removed if
     * (1) its child appears in <code>excludeVertexes</code>, (2) its relation appears in
     * <code>excludeRelations</code>, or (3) the edge has the root as parent and its relation
     * appears in <code>excludeRelationsTop</code>. */
    private static void removeEdges(SemanticGraph graph, IndexedWord root, Collection<IndexedWord> excludeVertexes,
                                    Collection<GrammaticalRelation> excludeRelations, Collection<GrammaticalRelation> excludeRelationsTop) {
        if (!excludeVertexes.contains(root)) {
            Set<SemanticGraphEdge> edgesToRemove = new HashSet<>();
            subgraph(graph, root, excludeVertexes, excludeRelations, excludeRelationsTop, edgesToRemove, 0);
            for (SemanticGraphEdge edge : edgesToRemove) {
                graph.removeEdge(edge);
            }
        }
    }

    /** Removes some edges from the given semantic graph.
     * 
     * This method traverses the semantic graph starting from the given root. An edge is removed if
     * its child appears in <code>excludeVertexes</code>. */
    public static void removeEdges(SemanticGraph graph, IndexedWord root, Collection<IndexedWord> excludeVertexes) {
        removeEdges(graph, root, excludeVertexes, Collections.emptySet(), Collections.emptySet());
    }

    /** Removes some edges from the given semantic graph.
     * 
     * This method traverses the semantic graph starting from the given root. An edge is removed if
     * its relation appears in <code>excludeRelations</code> or the edge has the root as parent and
     * its relation appears in <code>excludeRelationsTop</code>. 
     */
    public static void removeEdges(SemanticGraph graph, IndexedWord root, Collection<GrammaticalRelation> excludeRelations,
            Collection<GrammaticalRelation> excludeRelationsTop) {
        removeEdges(graph, root, Collections.emptySet(), excludeRelations, excludeRelationsTop);
    }

    /** Implementation for
     * {@link #removeEdges(SemanticGraph, IndexedWord, Collection, Collection, Collection)} */
    private static int subgraph(SemanticGraph graph, IndexedWord root, Collection<IndexedWord> excludeVertexes,
            Collection<GrammaticalRelation> excludeRelations, Collection<GrammaticalRelation> excludeRelationsTop,
            Collection<SemanticGraphEdge> edgesToRemove, int counter) {
        
        /* TODO: In some sentences there is infinite recursion. Dirty fix to stop it. 
         
         Example sentence:
         "Policies on electronic tickets differ ''from airline to airline and airport to airport,'' said Ms. McInerney, 
         whose group is working with the airline industry on e-ticket policies and the matter of standardizing itineraries 
         and receipts, perhaps with a universal template to create more readily verifiable printouts that carry uniform 
         information like a ticket number that can be matched to an airline computer reservation."
 
         */
        counter++;
        if (counter > MAX_RECURSION_ITERATIONS){
            return counter;
        }

        List<SemanticGraphEdge> edges = graph.getOutEdgesSorted(root);
        for (SemanticGraphEdge e : edges) {
            IndexedWord child = e.getDependent();
            if (excludeVertexes.contains(child) || excludeRelations.contains(e.getRelation())
                    || excludeRelationsTop.contains(e.getRelation())) {
                edgesToRemove.add(graph.getEdge(root, child));
            } else {
                counter = subgraph(graph, child, excludeVertexes, excludeRelations, 
                        Collections.emptySet(), edgesToRemove, counter);
            }
        }
        
        return counter;
    }

    /** Return a set of vertexes to be excluded according to a given collection of grammatical relations */
    public static Set<IndexedWord> exclude(SemanticGraph semanticGraph, Collection<GrammaticalRelation> rels, IndexedWord root) {
        Set<IndexedWord> exclude = new TreeSet<>();
        List<SemanticGraphEdge> outedges = semanticGraph.getOutEdgesSorted(root);
        for (SemanticGraphEdge edge : outedges) {
            if (containsAncestor(rels, edge)) {
                exclude.add(edge.getDependent());
            }
        }
        return exclude;
    }

    /** Check if an edge is descendant of any grammatical relation in the given set */
    private static boolean containsAncestor(Collection<GrammaticalRelation> rels, SemanticGraphEdge edge) {
        for (GrammaticalRelation rel : rels) {
            if (rel.isAncestor(edge.getRelation()))
                return true;
        }
        return false;
    }

    /**
     * The dependency parse might contain some special tokens (or whole words) which we don't want. 
     * Filter those out. 
     * @return boolean: if true, the token needs to be filtered, false -> otherwise
     */
    public static boolean filterTokens(IndexedWord word){
        return word.word().equals(".") || word.word().equals(",") || word.word().equals("-RRB-") || 
                word.word().equals("-LRB-") || word.word().equals("\"") || word.word().equals("\'\'") || 
                word.word().equals("``") || word.word().equals(";") || word.word().equals(":") || 
                word.word().equals("-") || (word.word().equals("'") && !word.tag().equals("POS")) || 
                word.word().equals("!") || word.word().equals("--") || word.word().equals("`") || 
                word.word().equals("?") || word.word().equals("-RCB-") || word.word().equals("-LCB-");
    }
}