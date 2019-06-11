
package de.uni_mannheim.clausie.proposition;

import java.util.HashSet;
import java.util.Set;

import de.uni_mannheim.clausie.constituent.Constituent;
import de.uni_mannheim.clausie.phrase.Phrase;
import de.uni_mannheim.constant.NE_TYPE;
import de.uni_mannheim.constant.SEPARATOR;

import de.uni_mannheim.utils.coreNLP.WordUtils;
import edu.stanford.nlp.ling.IndexedWord;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

/** Stores a proposition.
 *
 * @author Luciano del Corro
 * @author Kiril Gashteovski
 *
 */
public class Proposition {

	/** Constituents of the proposition */
	private ObjectArrayList<Phrase> phrases = new ObjectArrayList<>();

	/** Constituents of the proposition (as n-ary phrases) **/
	private ObjectArrayList<Phrase> naryPhrases = new ObjectArrayList<>();

	/** The n-ary phrases' constituent types **/
	private ObjectArrayList<Constituent.Type> constituentTypes = new ObjectArrayList<>();

	/** Position of optional constituents */
	private Set<Integer> optional = new HashSet<>();

	// TODO: types of constituents (e.g., optionality) sentence ID etc.

	public Proposition() {
	}

	/** Returns a list of constituents of the proposition */
	public ObjectArrayList<Phrase> getConstituents(){
		return this.phrases;
	}

	/** Returns the subject of the proposition */
	public Phrase subject() {
		return this.phrases.get(0);
	}

	/** Returns the relation of the proposition */
	public Phrase relation() {
		return phrases.get(1);
	}

	/** Returns the object of the proposition (should be used when working with triples only!) */
	public Phrase object(){
		return phrases.get(2);
	}

	/** Sets the relation of the proposition */
	public void setRelation(Phrase rel){
		phrases.set(1, rel);
	}

	/** Returns a constituent in a given position */
	public Phrase argument(int i) {
		return phrases.get(i + 2);
	}

	public void addConstituentType(Constituent.Type t) {
		this.constituentTypes.add(t);
	}

	public ObjectArrayList<Constituent.Type> getConstituentTypes() {
		return constituentTypes;
	}

	/**
	 * Given a proposition, this function turns it into a "sentence" by concatenating the constituents' strings
	 * @return preposition's words concatenated in a space-separated string
	 */
	public String propositionToString(){
		StringBuilder sb = new StringBuilder();
		for (Phrase phrase : phrases) {
			sb.append(phrase.getWords());
			sb.append(SEPARATOR.SPACE);
		}
		return sb.toString().trim();
	}

	public ObjectArrayList<Phrase> getPhrases(){
		return this.phrases;
	}
	public void addPhrase (Phrase p){
		this.phrases.add(p);
		this.naryPhrases.add(new Phrase(p));
	}
	public void setPhrase(int i, Phrase p){
		this.phrases.set(i, p);
	}
	public void setPhrases(ObjectArrayList<Phrase> phrases) {
		this.phrases = phrases;
	}
	public void setNaryPhrases(ObjectArrayList<Phrase> naryPhrases) {
		this.naryPhrases = naryPhrases;
	}
	public void setConstituentTypes(ObjectArrayList<Constituent.Type> types) {
		this.constituentTypes = types;
	}

	/** Add index of an optional constituent **/
	public void addOptionalConstituentIndex(int i){
		this.optional.add(i);
	}
	/** Clear the set of optional constituent indices **/
	public void clearOptionalConstituentIndicesSet(){
		this.optional.clear();
	}

	/**
	 * Given a constituent index i, push it to the relation of the proposition
	 * @param i: push the i-th phrase to the relation of the proposition
	 */
	private void pushConstituentToRelation(int i){
		this.pushConstituentAtoConstituentB(i, 1);
	}

	/**
	 * Givan a constituent indices a and b, push constituent a to constituent b
	 * @param a constituent a index
	 * @param b constituent b index
	 */
	private void pushConstituentAtoConstituentB(int a, int b) {
		// New phrase for b. The root of the relational phrase is the verb by default
		Phrase phraseB = new Phrase();
		phraseB.setHeadWord(this.phrases.get(b).getHeadWord());

		// Push
		phraseB.addWordsToList(this.phrases.get(b).getWordList().clone());
		phraseB.addWordsToList(this.phrases.get(a).getWordList().clone());
		this.setPhrase(b, phraseB);

		//Set  if processed by conjunction
		//XXX
		phraseB.setProcessedConjunction(this.getPhrases().get(a).isProcessedConjunction());
		phraseB.setConjWord(this.getPhrases().get(a).getConjWord());


		// Clean the i-th constituent
		this.phrases.get(a).getWordList().clear();
	}

	/**
	 * Given a proposition and a list of constituency types (corresponding the phrases of the proposition),
	 * push the constituents to the relation if needed
	 * @param types constituency types
	 */
	private void pushConstituentsToRelation(ObjectArrayList<Constituent.Type> types){
		// Don't break named entities

		// Push constituents to the relation if the 4th constituent is an adverbial
		// (for SVA(A), SVC(A), SVO(A), SVOA)
		if (types.get(3) == Constituent.Type.ADVERBIAL){
			// If the adverbial is consisted of one adverb, don't push the previous constituent
			if (this.phrases.get(3).getWordList().size() > 1) {
				// If CCOMP don't push it
				if (types.get(2) == Constituent.Type.CCOMP) {
					return;
				}
				pushConstituentToRelation( 2);
			}
			// If the adverbial is consisted of one adverb, push the adverb to the relation
			else if (this.phrases.get(3).getWordList().size() == 1){
				if (WordUtils.isAdverb(this.phrases.get(3).getWordList().get(0)))
					pushConstituentToRelation(3);
				else
					pushConstituentToRelation(2);
			}
		}
		// If the 3rd constituent is an indirect/direct object or an adverbial (for SVOO/SVOC, SVOA)
		else if (types.get(2) == Constituent.Type.IOBJ || types.get(2) == Constituent.Type.DOBJ ||
				types.get(2) == Constituent.Type.ADVERBIAL){
			pushConstituentToRelation(2);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		String sep = "(";

		for (int i=0; i < phrases.size(); i++) {
			String constituent = phrases.get(i).getWords();
			sb.append(sep);
			sep = ", ";
			sb.append("\"");
			sb.append(constituent);
			sb.append("\"");
			if (optional.contains(i)) {
				sb.append("?");
			}
		}
		sb.append(")");
		return sb.toString();
	}

	@Override
	public Proposition clone() {
		Proposition clone = new Proposition();
		clone.phrases = new ObjectArrayList<>(this.phrases.clone());
		clone.optional = new HashSet<>(this.optional);
		return clone;
	}

	public ObjectArrayList<Phrase> getNaryPhrases() {
		return naryPhrases;
	}

	/**
	 * If the proposition is n-ary tuple where n > 3, then convert it into a triple, and push the
	 * necessary constituents to the relation (thus making the triple a "compact triple")
	 */
	public void convertNAryPropositionToCompactTriple() {
		if (this.phrases.size() > 3) {

			this.pushConstituentsToRelation();
			this.mergeNAryTupleToTriple();

		}
	}

	private void pushConstituentsToRelation() {
		// TODO: java.lang.ArrayIndexOutOfBoundsException: -1 (rarely happens, but investigate; nlp-linked-5)
		if (this.phrases.get(2).getWordList().size() == 0 || this.phrases.get(3).getWordList().size() == 0) {
			this.pushConstituentsToRelation(this.constituentTypes);
			return;
		}

		IndexedWord w1 = this.phrases.get(2).getWordList().get(this.phrases.get(2).getWordList().size() - 1);
		IndexedWord w2 = this.phrases.get(3).getWordList().get(0);
		boolean areNERs = (!w1.ner().equals(NE_TYPE.NO_NER) && (w1.ner().equals(w2.ner())));

		// Push the necessary constituents to the relation (unless they are one NER
		if (areNERs) {
			this.pushConstituentAtoConstituentB(3, 2);
		} else {
			this.pushConstituentsToRelation(this.getConstituentTypes());
		}
	}

	private void mergeNAryTupleToTriple() {
		// Merge the rest of the n-ary tuple to the 3rd constituent (making it a triple)
		Phrase argPhrase = new Phrase();
		argPhrase.setHeadWord(this.phrases.get(2).getHeadWord());
		for (int i = 2; i < this.phrases.size(); i++) {
			argPhrase.addWordsToList(this.phrases.get(i).getWordList().clone());
		}

		//Set  if processed by conjunction)
		//XXX
		if(this.getPhrases().get(3).getWordList().size()!=0) {
			argPhrase.setProcessedConjunction(this.getPhrases().get(3).isProcessedConjunction());
		}
		//XXX
		if(this.getPhrases().get(3).getWordList().size()==0){
			argPhrase.setProcessedConjunction(this.getPhrases().get(2).isProcessedConjunction());
		}

		this.setPhrase(2, argPhrase);
		for (int i = this.phrases.size() - 1; i > 2; i--) {
			this.phrases.remove(i);
		}
	}
}
