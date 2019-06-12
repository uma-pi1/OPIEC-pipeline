package demo;

import de.uni_mannheim.minie.MinIE;
import de.uni_mannheim.minie.annotation.AnnotatedProposition;

import de.uni_mannheim.utils.coreNLP.NLPPipeline;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

public class Demo {
    public static void main(String args[]) {
        // Dependency parsing pipeline initialization
        StanfordCoreNLP parser = NLPPipeline.StanfordDepNNParser();

        // Input sentence
        String sentence = "Although Mr. Hague said the outcome proved that his view was now '' the settled will '' of the party , opponents argued that the turnout was only 58 percent and that , therefore , the support for the policy among all party members amounted to less than half , or 49.6 percent .";
        System.out.println(sentence);

        // Generate the extractions (With SAFE mode)
        MinIE minie = new MinIE(sentence, parser, MinIE.Mode.SAFE);

        // Print the extractions
        System.out.println("\nInput sentence: " + sentence);
        System.out.println("=============================");
        System.out.println("Extractions:");
        for (AnnotatedProposition ap: minie.getPropositions()) {
            System.out.println("\tTriple: " + ap.getTripleAsString());
            System.out.print("\tFactuality: " + ap.getFactualityAsString());
            if (ap.getAttribution().getAttributionPhrase() != null)
                System.out.print("\tAttribution: " + ap.getAttribution().toStringCompact());
            else
                System.out.print("\tAttribution: NONE");
            System.out.println("\n\t----------");
        }

        System.out.println("\n\nDONE!");
    }
}
