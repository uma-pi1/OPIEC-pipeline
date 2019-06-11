package de.uni_mannheim.clausie.constituent;

import de.uni_mannheim.clausie.phrase.Phrase;

/**
 * A phrase expression of a constituent. The constituent is represented as a Phrase
 *
 * @author Kiril Gashteovski
 *
 */
public class PhraseConstituent extends Constituent {
    /** The constituent as a phrase **/
    private Phrase phrase;
	
    /** Constructs a constituent with a specified textual representation and type. */
    public PhraseConstituent(Phrase p, Type type) {
        super(type);
        this.phrase = p;
        this.root = p.getHeadWord();
    }

    /** Returns a textual representation of the constituent. */
    public String rootString() {
        return this.phrase.getWords();
    }

    public Phrase getPhrase(){
        return this.phrase;
    }
}
