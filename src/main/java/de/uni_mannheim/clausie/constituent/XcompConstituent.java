package de.uni_mannheim.clausie.constituent;

import java.util.TreeSet;

import de.uni_mannheim.clausie.clause.Clause;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.semgraph.SemanticGraph;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

/** An {@code XcompConstituent} of a clause formed out of an xcomp.
 * 
 * Note that the xcomp relation refers to a clause with an external subject.
 * The constituent stores the set of clauses that can be derived from the xcomp 
 * clause. 
 *
 * @author Luciano del Corro
 * @author Kiril Gashteovski
 *
 */
public class XcompConstituent extends IndexedConstituent {
	
    /** Clauses derived from this constituent */
    private ObjectArrayList<Clause> clauses;
	
    private XcompConstituent() {		
    }

    /** Constructs a new constituent for the xcomp relation.
     * 
     * @param semanticGraph Semantic graph for this constituent ({@see #semanticGraph})
     * @param root The root vertex of this constituent ({@see {@link #root})
     * @param type type of this constituent 
     * @param clauses derived from this constituent
     */
    public XcompConstituent(SemanticGraph semanticGraph, IndexedWord root, Type type, ObjectArrayList<Clause> clauses) {
        super(semanticGraph, root, type);
        this.setClauses(clauses);
    }

    /** Returns the clauses derived from the constituent. */
    public ObjectArrayList<Clause> getClauses() {
        return clauses;
    }

    /** Sets the clauses derived from the constituent. */
    private void setClauses(ObjectArrayList<Clause> clauses) {
        this.clauses = clauses;
    }
	
    @Override
    public XcompConstituent clone() {
        XcompConstituent clone = new XcompConstituent();
        clone.type = type;
        clone.setSemanticGraph(new SemanticGraph(this.getSemanticGraph()));
        clone.root = this.getRoot();
        clone.setAdditionalVertexes(new TreeSet<>(this.getAdditionalVertexes()));
        clone.excludedVertexes = new TreeSet<>(this.excludedVertexes);
        clone.clauses = new ObjectArrayList<>(clauses);
        return clone;
    }
}
