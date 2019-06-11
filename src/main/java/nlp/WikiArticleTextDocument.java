package nlp;

import java.util.ArrayList;
import java.util.List;

import avroschema.linked.WikiLink;
import avroschema.linked.SentenceLinked;
import avroschema.linked.Span;
import avroschema.linked.TokenLinked;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.BasicDependenciesAnnotation;
import edu.stanford.nlp.util.CoreMap;

public class WikiArticleTextDocument {
    /** The text from the document that needs to be processed **/
    private String text;
    /** The CoreNLP pipeline **/
    private StanfordCoreNLP pipeline;
    /** Each document is consisted of a list of sentences, structured according to the AVRO schema of a sentence **/
    private List<SentenceLinked> sentences;

    /** Constructor given the text string and the CoreNLP pipeline **/
    public WikiArticleTextDocument(String text, StanfordCoreNLP pipeline){
        this.text = text;
        this.pipeline = pipeline;
    }

    // Setters
    public void setPipeline (StanfordCoreNLP pipeline){
        this.pipeline = pipeline;
    }
    public void setText (String text){
        this.text = text;
    }

    // Getters
    public List<SentenceLinked> getSentences(){
        return this.sentences;
    }

    /**
     * Assuming the pipeline is already initialized, annotate the sentences
     */
    public void annotateSentences() {
        Annotation document = new Annotation(this.text);
        try {
            this.pipeline.annotate(document);
        } catch(IllegalArgumentException e ){
            this.sentences = new ArrayList<>();
            return;
        }

        // Get the sentences from a text in both CoreMap and Sentence lists
        List<CoreMap> coreMapSentences = document.get(SentencesAnnotation.class);

        this.sentences = new ArrayList<>();

        // Default WikiLink which does not contain any link at all
        WikiLink defaultWL = new WikiLink();
        defaultWL.setOffsetBeginInd(-1);
        defaultWL.setOffsetEndInd(-1);
        defaultWL.setPhrase("");
        defaultWL.setWikiLink("");

        // For each sentence, add the fields for the schema (POS, NER, DP, ... )
        int sentID = 0;

        int sentStartIndex = 0;
        int sentEndIndex = 0;
        for (CoreMap sentence: coreMapSentences){
            // Trim the long sentences before starting to annotate
            if (sentence.toString().split(" ").length > 100) {
                continue;
            }

            // Get the tokens
            List<CoreLabel> tokens = sentence.get(TokensAnnotation.class);
            List<TokenLinked> toks = new ArrayList<>();

            // For each token, add NEs, POS, span
            int lastEndIndex = 0;
            for (CoreLabel t : tokens){
                TokenLinked tok = new TokenLinked();
                tok.setLemma(t.lemma());
                tok.setWord(t.word());
                tok.setNer(t.ner());
                tok.setPos(t.tag());
                tok.setIndex(t.index());
                tok.setWLink(defaultWL);

                Span s = new Span();
                s.setStartIndex(t.beginPosition() - sentEndIndex);
                s.setEndIndex(t.endPosition() - sentEndIndex);
                lastEndIndex = t.endPosition() - sentEndIndex;
                tok.setSpan(s);

                toks.add(tok);
            }
            sentEndIndex += lastEndIndex;

            // Parse the sentence
            SemanticGraph semanticGraph = sentence.get(BasicDependenciesAnnotation.class);

            // Add sentence ID, the typed dependencies of the sentence and the semantic graph
            SentenceLinked sent = new SentenceLinked();
            Span sentSpan = new Span();
            sentSpan.setStartIndex(sentStartIndex);
            sentStartIndex = sentEndIndex + 1;
            sentSpan.setEndIndex(sentEndIndex);
            sentEndIndex++;
            sent.setSpan(sentSpan);
            sent.setTokens(toks);
            sent.setSId(sentID);
            sent.setSg(semanticGraph.toCompactString());
            sent.setDp(semanticGraph.typedDependencies().toString());
            sentID++;

            // Add the processed sentence to the list of sentences
            this.sentences.add(sent);
        }
    }
}
