package de.uni_mannheim.minie.confidence;

import de.uni_mannheim.clausie.ClausIE;
import de.uni_mannheim.minie.MinIE;
import de.uni_mannheim.minie.annotation.factuality.Modality;
import de.uni_mannheim.minie.annotation.factuality.Polarity;
import de.uni_mannheim.utils.coreNLP.CoreNLPUtils;
import de.uni_mannheim.utils.coreNLP.NLPPipeline;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import java.io.*;
import java.util.ArrayList;

/**
 * @author Sebastian Wanner
 * @author Kiril Gashteovski
 */
public class Demo {
    // Reading and writing file names
    public static final String READ_FILE_NAME_1 = "data/nyt10k/NYT10k.txt";

    public static final String WRITE_FILE_NAME_1 = "data/confidence/nyt10k_MinIE_confidence_ExecutionTime.csv";

    // Buffered readers and writers
    private static BufferedReader br;


    public static void main(String args[]) throws IOException {
        ArrayList<String> writeFiles = new ArrayList<>();
        writeFiles.add(WRITE_FILE_NAME_1);

        ArrayList<String> readFiles = new ArrayList<>();
        readFiles.add(READ_FILE_NAME_1);

        for(int fileNr = 0; fileNr < readFiles.size(); fileNr++) {

            // Initialize the parser
            StanfordCoreNLP parser = NLPPipeline.StanfordDepNNParser();

            // Initialize ClausIE + MinIE objects
            ClausIE clausIE = new ClausIE();
            clausIE.getOptions().print(System.out, "# ");

            MinIE minie = new MinIE();

            // Reading file
            br = new BufferedReader(new FileReader(readFiles.get(fileNr)));
            String line = br.readLine();

            int counter = 0;

            PrintWriter pw = new PrintWriter(new File(writeFiles.get(fileNr)));
            StringBuilder columnBuilder = new StringBuilder();
            String ColumnList = "sentence#ExtractionTime\n";
            columnBuilder.append(ColumnList);
            pw.write(columnBuilder.toString());

            // Extract the propositions from each sentence (line) from the reading file
            System.out.println("Extracting propositions ... ");
            do {
                StringBuilder builder = new StringBuilder();
                builder.append(line + "#");
                counter++;
                System.out.println("LINE: " + counter);
                System.out.println("Sentence: " + line);

                //clear clausIE
                clausIE.clear();

                // Parse the sentence and measure the parsing time
                clausIE.setSemanticGraph(CoreNLPUtils.parse(parser, line));

                // Clause detection + propositions generation
                clausIE.detectClauses();
                clausIE.generatePropositions(clausIE.getSemanticGraph());

                //  Start minimizing
                long startTime = System.currentTimeMillis();
                minie.clear();
                minie.setSemanticGraph(clausIE.getSemanticGraph());
                minie.setPropositions(clausIE);
                minie.annotatePolarity();
                minie.annotateModality();
                minie.minimizeSafeMode();
                minie.removeDuplicates();

                //predict Confidence Score
                minie.predictConfidenceScore(clausIE);
                long stopTime = System.currentTimeMillis();
                builder.append(Long.toString(stopTime - startTime));
                builder.append("\n");
                pw.write(builder.toString());
                pw.flush();


                for (int i = 0; i < minie.getPropositions().size(); i++){
                    for (int j = 0; j < minie.getProposition(i).getTriple().size(); j++){
                        System.out.print("\"" + minie.getProposition(i).getTriple().get(j).getWords() + "\"\t");
                    }

                    System.out.print("Confidence Score:" + minie.getProposition(i).getConfidence() + "\t");

                    // Write the attribution if found any
                    if (minie.getProposition(i).getAttribution().getAttributionPhrase() != null){
                        System.out.print("\t Source:" + minie.getProposition(i).getAttribution());
                    }
                    // Write polarity if negation is found
                    if (minie.getProposition(i).getPolarity().getType() == Polarity.Type.NEGATIVE){
                        System.out.print("\t Polarity:" + minie.getProposition(i).getPolarity().toString());
                    }
                    // Write modality if possibility is found
                    if (minie.getProposition(i).getModality().getModalityType() == Modality.Type.POSSIBILITY){
                        System.out.print("\t Modality:" + minie.getProposition(i).getModality().toString());
                    }
                    System.out.println("\nTo string: " + minie.getProposition(i).toString());
                    System.out.println();
                }
                line = br.readLine();
            } while (line != null);


            // Close the buffers
            br.close();
            System.out.println("\nDONE!!!\n" + readFiles.get(fileNr).toString());

        }

    }
}
