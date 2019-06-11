package avroschema.triple;

import avroschema.linked.*;

import avroschema.util.TokenLinkedUtils;
import avroschema.util.TripleLinkedUtils;
import de.uni_mannheim.constant.POS_TAG;
import de.uni_mannheim.minie.annotation.AnnotatedProposition;
import de.uni_mannheim.minie.annotation.Quantity;

import de.uni_mannheim.minie.annotation.SpaTe.space.Space;
import de.uni_mannheim.minie.annotation.SpaTe.time.Time;
import edu.stanford.nlp.ling.IndexedWord;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WikiTripleLinked {
    private AnnotatedProposition aProp;
    private SentenceLinked s;
    private TripleLinked triple;
    private WikiArticleLinkedNLP article;
    private HashMap<Integer, WikiLink> tokenLinks;

    public WikiTripleLinked(AnnotatedProposition aProp, SentenceLinked s, WikiArticleLinkedNLP a, HashMap<Integer, WikiLink> tokensLinks) {
        this.aProp = aProp;
        this.s = s;
        this.article = a;
        this.triple = new TripleLinked();
        this.tokenLinks = tokensLinks;
    }

    private ObjectArrayList<TokenLinked> indexedWordsListToTokensList(ObjectArrayList<IndexedWord> words) {
        ObjectArrayList<TokenLinked> tokens = new ObjectArrayList<>();
        for (IndexedWord w: words) {
            TokenLinked t = new TokenLinked();
            t.setIndex(w.index());
            t.setLemma(w.lemma());
            t.setNer(w.ner());
            t.setPos(w.tag());
            t.setWord(w.word());
            if (this.tokenLinks.get(t.getIndex()) == null) {
                WikiLink wl = new WikiLink();
                wl.setWikiLink("");
                wl.setPhrase("");
                wl.setOffsetEndInd(-1);
                wl.setOffsetBeginInd(-1);
                t.setWLink(wl);
            } else {
                t.setWLink(this.tokenLinks.get(t.getIndex()));
            }

            // Handle implicit extractions' words
            if (t.getIndex() < 0) {
                Span span = new Span();
                span.setStartIndex(-1);
                span.setEndIndex(-1);
                t.setSpan(span);
            }

            for (TokenLinked sentToken: this.s.getTokens()) {
                if (w.index() == sentToken.getIndex()) {
                    t.setSpan(sentToken.getSpan());
                    break;
                }
            }

            tokens.add(t);
        }
        return tokens;
    }

    /**
     * Set factuality, quantities and attribution
     */
    private void setTripleSemanticAnnotations() {
        this.setFactuality();
        this.setQuantities();
        this.setAttribution();
    }

    /**
     * Set polarity and modality
     */
    private void setFactuality() {
        this.triple.setModality(aProp.getModality().getModalityType().toString());
        this.triple.setPolarity(aProp.getPolarity().getType().toString());
    }

    private void setQuantities() {
        ObjectArrayList<Quantity> quantities = aProp.getAllQuantities();
        Map<String, String> qMap = new HashMap<>();
        if (!quantities.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (Quantity q : quantities) {
                for (IndexedWord w : q.getQuantityWords()) {
                    sb.append(w.word());
                    sb.append(" ");
                }
                qMap.put(q.getId(), sb.toString().trim());
                sb.setLength(0);
            }
        }
        this.triple.setQuantities(qMap);
    }

    private void setTripleTime() {
        // Set triple time
        if (!this.aProp.getTime().getTimes().isEmpty()) {
            List<TimeLinked> timesLinked = new ArrayList<>();
            for (Time pTime: aProp.getTime().getTimes()) {
                TimeLinked lTime = new TimeLinked();
                lTime.setCoreWords(this.getCoreTemporalWords(pTime));
                lTime.setAllWords(this.getAllTemporalWords(pTime));
                lTime.setDisambiguated(pTime.getValue());
                lTime.setId(pTime.getId());
                lTime.setPreMods(this.getPreMods(pTime));
                lTime.setPostMods(this.getPostMods(pTime));
                if (pTime.getPredicate().index() > 0) {
                    lTime.setPredicate(this.s.getTokens().get(pTime.getPredicate().index() - 1));
                }
                lTime.setTimex3Type(pTime.getType().toString());
                lTime.setTimex3Xml(pTime.getTimex3Xml().toString());
                timesLinked.add(lTime);
            }
            this.triple.setTimeLinked(timesLinked);
        }

        // Set subj/rel/obj time
        Time subjTime = this.aProp.getSubject().getTime();
        Time relTime = this.aProp.getRelation().getTime();
        Time objTime = this.aProp.getObject().getTime();
        this.triple.setSubjTimeLinked(this.getTimeLinked(subjTime.getAllTemporalWords(), this.aProp.getSubject().getTemporallyModifiedWord()));
        this.triple.setRelTimeLinked(this.getTimeLinked(relTime.getAllTemporalWords(), this.aProp.getRelation().getTemporallyModifiedWord()));
        this.triple.setObjTimeLinked(this.getTimeLinked(objTime.getAllTemporalWords(), this.aProp.getObject().getTemporallyModifiedWord()));
    }

    private void setTripleSpace() {
        // Set triple space
        if (!this.aProp.getSpace().getSpaces().isEmpty()) {
            List<SpaceLinked> spaces = new ArrayList<>();
            for (Space space: this.aProp.getSpace().getSpaces()) {
                SpaceLinked spaceLinked = new SpaceLinked();
                spaceLinked.setAllWords(this.getAllSpaceTokens(space));
                spaceLinked.setPreMods(this.getSpacePreMods(space));
                spaceLinked.setPostMods(this.getSpacePostMods(space));
                spaceLinked.setCoreWords(this.getSpaceCoreWords(space));
                if (space.getPredicate().index() == -2) {
                    spaceLinked.setPredicate(TokenLinkedUtils.toTokenLinked(space.getPredicate()));
                }
                else {
                    spaceLinked.setPredicate(this.triple.getSentenceLinked().getTokens().get(space.getPredicate().index() - 1));
                }
                spaces.add(spaceLinked);
            }
            this.triple.setSpaceLinked(spaces);
        }

        // Set subj/rel/obj for space
        Space subjSpace = this.aProp.getSubject().getSpace();
        Space relSpace = this.aProp.getRelation().getSpace();
        Space objSpace = this.aProp.getObject().getSpace();
        this.triple.setSubjSpaceLinked(this.getSpaceLinked(subjSpace.getAllSpatialWords(), this.aProp.getSubject().getSpatiallyModifiedWord()));
        this.triple.setRelSpaceLinked(this.getSpaceLinked(relSpace.getAllSpatialWords(), this.aProp.getRelation().getSpatiallyModifiedWord()));
        this.triple.setObjSpaceLinked(this.getSpaceLinked(objSpace.getAllSpatialWords(), this.aProp.getObject().getSpatiallyModifiedWord()));
    }

    private List<TokenLinked> getAllSpaceTokens(Space space) {
        List<TokenLinked> spaceTokens = new ArrayList<>();
        for (IndexedWord w: space.getAllSpatialWords()) {
            spaceTokens.add(this.triple.getSentenceLinked().getTokens().get(w.index() - 1));
        }
        return spaceTokens;
    }

    private List<TokenLinked> getSpacePreMods(Space space) {
        List<TokenLinked> spacePreMods = new ArrayList<>();
        for (IndexedWord w: space.getPreMods()) {
            spacePreMods.add(this.triple.getSentenceLinked().getTokens().get(w.index() - 1));
        }
        return spacePreMods;
    }

    private List<TokenLinked> getSpacePostMods(Space space) {
        List<TokenLinked> spacePostMods = new ArrayList<>();
        for (IndexedWord w: space.getPostMods()) {
            spacePostMods.add(this.triple.getSentenceLinked().getTokens().get(w.index() - 1));
        }
        return spacePostMods;
    }

    private List<TokenLinked> getSpaceCoreWords(Space space) {
        List<TokenLinked> spaceCoreWords = new ArrayList<>();
        for (IndexedWord w: space.getCoreSpatialWords()) {
            spaceCoreWords.add(this.triple.getSentenceLinked().getTokens().get(w.index() - 1));
        }
        return spaceCoreWords;
    }

    /**
     * Given a list of temporal words, return an object TemporalWordLinked
     * @param temporalWords list of temporal words
     * @param temporallyModifiedWord temporally modified word
     * @return temporal word linked object
     */
    private TemporalWordLinked getTimeLinked(ObjectArrayList<IndexedWord> temporalWords, IndexedWord temporallyModifiedWord) {
        TemporalWordLinked tw = new TemporalWordLinked();
        tw.setModifiedWord(temporallyModifiedWord.lemma());
        TimeLinked tl = new TimeLinked();
        List<TokenLinked> tempTokens = new ArrayList<>();
        for (IndexedWord w: temporalWords) {
            tempTokens.add(this.triple.getSentenceLinked().getTokens().get(w.index() - 1));
        }
        tl.setAllWords(tempTokens);
        tw.setTimeLinked(tl);
        return tw;
    }

    /**
     * Given a list of spatial words, return an object SpatialWordLinked
     * @param spatialWords list of spatial words
     * @param spatiallyModifiedWord spatially modified word
     * @return spatial word linked object
     */
    private SpatialWordLinked getSpaceLinked(ObjectArrayList<IndexedWord> spatialWords, IndexedWord spatiallyModifiedWord) {
        SpatialWordLinked sw = new SpatialWordLinked();
        sw.setModifiedWord(spatiallyModifiedWord.lemma());
        SpaceLinked sl = new SpaceLinked();
        List<TokenLinked> spaceTokens = new ArrayList<>();
        for (IndexedWord w: spatialWords) {
            spaceTokens.add(this.triple.getSentenceLinked().getTokens().get(w.index() - 1));
        }
        sl.setAllWords(spaceTokens);
        sw.setSpace(sl);
        return sw;
    }

    /**
     * @param pTime proposition time annotation
     * @return list of TokenLinked tokens from pTime (for core temporal words)
     */
    private List<TokenLinked> getCoreTemporalWords(Time pTime) {
        List<TokenLinked> coreTempWords = new ArrayList<>();
        for (IndexedWord w: pTime.getCoreTemporalWords()) {
            coreTempWords.add(this.s.getTokens().get(w.index() - 1));
        }
        return coreTempWords;
    }

    /**
     * @param pTime proposition time annotation
     * @return list of TokenLinked tokens from pTime (for all temporal words)
     */
    private List<TokenLinked> getAllTemporalWords(Time pTime) {
        List<TokenLinked> allTempWords = new ArrayList<>();
        for (IndexedWord w: pTime.getAllTemporalWords()) {
            allTempWords.add(this.s.getTokens().get(w.index() - 1));
        }
        return allTempWords;
    }

    /**
     * @param pTime proposition time annotation
     * @return list of pre-modifier tokens from pTime
     */
    private List<TokenLinked> getPreMods(Time pTime) {
        List<TokenLinked> premods = new ArrayList<>();
        for (IndexedWord w: pTime.getPreMods()) {
            premods.add(this.s.getTokens().get(w.index() - 1));
        }
        return premods;
    }

    /**
     * @param pTime proposition time annotation
     * @return list of post-modifier tokens from pTime
     */
    private List<TokenLinked> getPostMods(Time pTime) {
        List<TokenLinked> postmods = new ArrayList<>();
        for (IndexedWord w: pTime.getPostMods()) {
            postmods.add(this.s.getTokens().get(w.index() - 1));
        }
        return postmods;
    }

    /**
     * Set attribution with all of its annotations
     */
    private void setAttribution() {
        if (aProp.getAttribution().getAttributionPhrase() != null) {
            AttributionLinked attr = new AttributionLinked();
            attr.setPredicate(aProp.getAttribution().getPredicateVerb().lemma());
            attr.setModality(aProp.getAttribution().getModalityType().toString());
            attr.setPolarity(aProp.getAttribution().getPolarityType().toString());
            StringBuilder sb = new StringBuilder();

            for (IndexedWord w: aProp.getAttribution().getAttributionPhrase().getWordList()) {
                sb.append(w.word());
                sb.append(" ");
                //         resolveCorefLemma(aProp.getSubject().getWordList(), corefChain, article.getRepresentativeMention());
                attr.setAttributionPhraseOriginal(sb.toString().trim());
            }

            this.triple.setAttributionLinked(attr);
        }
    }

    private void setTripleDroppedWords() {
        ObjectArrayList<IndexedWord> droppedWordsSubj = new ObjectArrayList<>();
        ObjectArrayList<IndexedWord> droppedWordsRel = new ObjectArrayList<>();
        ObjectArrayList<IndexedWord> droppedWordsObj = new ObjectArrayList<>();
        droppedWordsSubj.addAll(aProp.getSubject().getDroppedWords());
        droppedWordsRel.addAll(aProp.getRelation().getDroppedWords());
        droppedWordsObj.addAll(aProp.getObject().getDroppedWords());
        this.triple.setDroppedWordsSubject(indexedWordsListToTokensList(droppedWordsSubj));
        this.triple.setDroppedWordsRelation(indexedWordsListToTokensList(droppedWordsRel));
        this.triple.setDroppedWordsObject(indexedWordsListToTokensList(droppedWordsObj));

        ObjectArrayList<IndexedWord> negativeWords = new ObjectArrayList<>();
        ObjectArrayList<IndexedWord> certaintyWords = new ObjectArrayList<>();
        ObjectArrayList<IndexedWord> possibilityWords = new ObjectArrayList<>();
        negativeWords.addAll(aProp.getPolarity().getNegativeWords());
        possibilityWords.addAll(aProp.getModality().getPossibilityWords());
        certaintyWords.addAll(aProp.getModality().getCertaintyWords());
        this.triple.setNegativeWords(indexedWordsListToTokensList(negativeWords));
        this.triple.setPossibilityWords(indexedWordsListToTokensList(possibilityWords));
        this.triple.setCertaintyWords(indexedWordsListToTokensList(certaintyWords));
    }

    private void annotateTriple() {
        // Triple data
        this.triple.setSubject(indexedWordsListToTokensList(this.aProp.getSubject().getWordList()));
        this.triple.setRelation(indexedWordsListToTokensList(this.aProp.getRelation().getWordList()));
        this.triple.setObject(indexedWordsListToTokensList(aProp.getObject().getWordList()));

        // Triple annotations metadata
        this.setTripleSemanticAnnotations();
        this.setTripleDroppedWords();
    }

    public void populateTriple(String trpleID) {
        // Some triple metadata concerning the article
        this.triple.setSystem("MinIE-SpaTe");
        this.triple.setCorpus("Wiki");
        this.triple.setArticleId("Wiki_" + this.article.getUrl());
        this.triple.setTripleId(trpleID);
        this.triple.setExtractionType(this.aProp.getExtractionType());

        // Sentence metadata
        this.triple.setSentenceLinked(s);
        this.triple.setSentenceNumber(s.getSId().toString());

        // Triple data
        this.annotateTriple();
        this.setTripleTime();
        this.setTripleSpace();

        // Restructure the triples
        this.structureBrokenLinks();

        // Confidence score
        this.triple.setConfidenceScore(this.aProp.getConfidence());
    }

    /**
     * Whenever we have parts of a link in the relation, and part in the object, just restructure the triple by pushing
     * the linked stuff back to the object.
     */
    public void structureBrokenLinks() {
        if (this.triple.getObject().isEmpty() || (this.triple.getRelation().size() == 1)) {
            return;
        }

        // t1 -> the last token of the relation; t2 -> the first token of the object
        TokenLinked t1 = this.triple.getRelation().get(this.triple.getRelation().size() - 1);
        TokenLinked t2 = this.triple.getObject().get(0);

        // Push linked tokens from relation back to the object if necessary
        if (TokenLinkedUtils.hasLink(t1) && TokenLinkedUtils.haveSameLink(t1, t2)) {
            boolean argsAreOneLink = this.areArgumentsOneLink();

            if (!argsAreOneLink) {
                String objLink = this.triple.getObject().get(0).getWLink().getWikiLink();
                List<TokenLinked> relRemWords = new ArrayList<>();
                for (int i = this.triple.getRelation().size() - 1; i >= 0; i--) {
                    if (triple.getRelation().get(i).getWLink().getWikiLink().equals(objLink)) {
                        relRemWords.add(0, triple.getRelation().get(i));
                    } else {
                        break;
                    }
                }

                if (this.triple.getRelation().size() == relRemWords.size()) {
                    return;
                }

                this.triple.getObject().addAll(0, relRemWords);
                this.triple.getRelation().removeAll(relRemWords);
                if (this.triple.getObject().get(0).getPos().equals(POS_TAG.IN) ||
                        this.triple.getObject().get(0).getPos().equals(POS_TAG.TO)) {
                    this.triple.getRelation().add(this.triple.getObject().get(0));
                    this.triple.getObject().remove(0);
                }
            }
        }
    }

    /**
     * Checks if all the words in the subject and the object are one same link
     * @return true if all the words in subj and obj are the same link (if no link - return false); false -> otherwise
     */
    public boolean areArgumentsOneLink() {
        boolean argsSameLink = false;
        if (this.isSubjectOneLink() && this.isObjectOneLink()) {
            if (TokenLinkedUtils.haveSameLink(this.triple.getSubject().get(0), this.triple.getObject().get(0))) {
                argsSameLink = true;
            }
        }
        return argsSameLink;
    }

    /**
     * For a list of tokens, evaluate if they all have the same link. If all of them have no links, then return false
     * @param tokens list of linked tokens
     * @return true -> if all tokens contain the same link; false -> otherwise
     */
    private boolean isOneLink(List<TokenLinked> tokens) {
        if (tokens.get(0).getWLink().getWikiLink().equals("")) {
            return false;
        }
        for (TokenLinked t: tokens) {
            if (!TokenLinkedUtils.haveSameLink(tokens.get(0), t)) {
                return false;
            }
        }
        return true;
    }

    public boolean isSubjectOneLink() {
        return this.isOneLink(this.triple.getSubject());
    }

    public boolean isObjectOneLink() {
        return this.isOneLink(this.triple.getObject());
    }

    public void populateCanonicalLinksMap(HashMap<String, String> redirectsMap) {
        this.triple.setCanonicalLinks(TripleLinkedUtils.getCanonicalLinksMap(triple, redirectsMap));
    }

    public TripleLinked getTripleLinked() {
        return this.triple;
    }
}
