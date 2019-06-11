package de.uni_mannheim.clausie.proposition;

import java.util.SortedSet;
import java.util.TreeSet;

import de.uni_mannheim.clausie.ClausIE;
import de.uni_mannheim.clausie.clause.Clause;
import de.uni_mannheim.clausie.constituent.Constituent;
import de.uni_mannheim.clausie.constituent.IndexedConstituent;
import de.uni_mannheim.clausie.constituent.PhraseConstituent;
import de.uni_mannheim.clausie.constituent.Constituent.Status;
import de.uni_mannheim.clausie.phrase.Phrase;

import edu.stanford.nlp.semgraph.SemanticGraph;


/**
 * Currently the default proposition generator generates 3-ary propositions out of a clause
 *
 * @author Luciano del Corro
 * @author Kiril Gashteovski
 *
 * */
public class DefaultPropositionGenerator extends PropositionGenerator {
    public DefaultPropositionGenerator(ClausIE clausIE) {
        super(clausIE);
    }

    /** 
     *  @param clause: the clause in which the proposition is generated (and added to the list of propositions in 'clause')
     *  @param sGraph: semantic graph of the sentence
     */
    @Override
    public void generate(Clause clause, SemanticGraph sGraph) {
        Proposition proposition = new Proposition();
        // Process subject
        if (clause.getSubject() > -1 && clause.getIncludedConstitsInds().getBoolean(clause.getSubject())) { // subject is -1 when there is an xcomp
            Phrase subjPhrase = generate(clause, clause.getSubject(), sGraph);
            Constituent subjConstituent = clause.getConstituents().get(clause.getSubject());
            subjPhrase.setHeadWord(subjConstituent.getRoot());
            proposition.addPhrase(new Phrase(subjPhrase));
            proposition.addConstituentType(Constituent.Type.SUBJECT);
        }

        // Process verb
        if (clause.getIncludedConstitsInds().getBoolean(clause.getVerbInd())) {
            Phrase relation = generate(clause, clause.getVerbInd(), sGraph);
            Constituent verb = clause.getConstituents().get(clause.getVerbInd());
            relation.setHeadWord(verb.getRoot());
            proposition.addPhrase(new Phrase(relation));
            proposition.addConstituentType(Constituent.Type.VERB);
        } else {
            throw new IllegalArgumentException();
        }

        // Process arguments
        SortedSet<Integer> sortedIndexes = new TreeSet<>();
        sortedIndexes.addAll(clause.getIobjectsInds());
        sortedIndexes.addAll(clause.getDobjectsInds());
        sortedIndexes.addAll(clause.getXcompsInds());
        sortedIndexes.addAll(clause.getCcompsInds());
        sortedIndexes.addAll(clause.getAcompsInds());
        sortedIndexes.addAll(clause.getAdverbialInds());
        if (clause.getComplementInd() >= 0)
            sortedIndexes.add(clause.getComplementInd());
        for (int index: sortedIndexes) {
            Constituent verbConstituent = clause.getConstituents().get(clause.getVerbInd());
            Constituent indexConstituent = clause.getConstituents().get(index);
            boolean isVerbIndexedConstituent = verbConstituent instanceof IndexedConstituent;
            boolean adverbialsContainIndex = clause.getAdverbialInds().contains(index);
            if (isVerbIndexedConstituent && adverbialsContainIndex && 
                    indexConstituent.getRoot().index() < verbConstituent.getRoot().index()) 
                continue;

            if (clause.getIncludedConstitsInds().getBoolean(index)) {
                Phrase argument = generate(clause, index, sGraph);
                argument.setHeadWord(clause.getConstituents().get(index).getRoot());
                proposition.addPhrase(new Phrase(argument));
                proposition.addConstituentType(clause.getConstituents().get(index).getType());
            }
        }

        // Process adverbials  before verb
        sortedIndexes.clear();
        sortedIndexes.addAll(clause.getAdverbialInds());
        for (Integer index : sortedIndexes) {
            Constituent verbConstituent = clause.getConstituents().get(clause.getVerbInd());
            Constituent indexConstituent = clause.getConstituents().get(index);
            boolean isVerbPhraseConstituent = verbConstituent instanceof PhraseConstituent;
            // If the verb is a TextConstituent or the current constituent's root index is greater than the
            // verb constituent's root index -> break  
            if (isVerbPhraseConstituent || (indexConstituent.getRoot().index() > verbConstituent.getRoot().index())) 
                break;
            if (clause.getIncludedConstitsInds().getBoolean(index)) {
                Phrase argument = generate(clause, index, sGraph);
                argument.setHeadWord(clause.getConstituents().get(index).getRoot());
                proposition.addPhrase(new Phrase(argument));
                proposition.addConstituentType(clause.getConstituents().get(index).getType());

                if (clause.getConstituentStatus(index, clausIE.getOptions()).equals(Status.OPTIONAL)) {
                    proposition.addOptionalConstituentIndex(proposition.getPhrases().size());
                }	
            }
        }

        // Make triple if specified + push necessary constituents to the relation
        if (!clausIE.getOptions().nary) {
            proposition.clearOptionalConstituentIndicesSet();
            proposition.convertNAryPropositionToCompactTriple();
        }

        // We are done
        clause.addProposition(proposition);
    }
}
