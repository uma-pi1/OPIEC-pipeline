package de.uni_mannheim.minie.main;


import edu.stanford.nlp.ling.IndexedWord;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;

import java.util.logging.Logger;

import de.uni_mannheim.utils.Dictionary;
import de.uni_mannheim.minie.MinIE;
import de.uni_mannheim.minie.annotation.AnnotatedProposition;
import de.uni_mannheim.utils.minie.Utils;

/**
 * Main class that acts as a console interface to the MinIE system
 *
 * @author Martin Achenbach
 * @author Kiril Gashteovski
 */
public class Main {
    /** used MinIE mode **/
    private static MinIE.Mode mode;

    /** console logger **/
    private final static Logger logger = Logger.getLogger(String.valueOf(Main.class));

    /**
     * main function to call from console with available options
     * @param args: console arguments
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        // init the optionParser
        OptionParser optionParser = initOptionParser();
        OptionSet options;
        // parse options
        try {
            options = optionParser.parse(args);
        } catch (OptionException e) {
            System.err.println(e.getMessage());
            System.out.println("");
            optionParser.printHelpOn(System.out);
            return;
        }

        //print help if need, if yes, break
        if (options.has("h")) {
            optionParser.printHelpOn(System.out);
            return;
        }

        // setup input and output
        logger.info("Setting up input and output streams...");
        InputStream in = getInputStream(options);
        OutputStream out = getOutputStream(options);
        BufferedReader din = new BufferedReader(new InputStreamReader(in));
        PrintStream dout = new PrintStream(out, true, "UTF-8");

        // get mode
        mode = Utils.getMode((String) options.valueOf("m"));
        logger.info("Mode set to " + mode);

        // initialize extractor
        Extractor extractor;


        if (mode == MinIE.Mode.DICTIONARY) {
            // load multi-word dictionaries if in dictionary mode
            Dictionary collocationDictionary = Utils.loadDictionary(options);
            extractor = new Extractor(collocationDictionary);
        } else {
            // if not use default constructor
            extractor = new Extractor();
        }

        extractor.setRefdate((String) options.valueOf("ref-date"));

        logger.info("\n\nSetup finished, ready to take input sentence:");

        // start analyzing
        String line;
        while ((line = din.readLine()) != null) {
            // skip empty lines
            if (line.isEmpty()) continue;

            // parse sentence
            MinIE result = extractor.analyzeSentence(line, mode);

            // print results from MinIE
            ObjectArrayList<AnnotatedProposition> propositions = result.getPropositions();
            dout.println("\nOutput:");
            if (options.has("p")) {
                result.getSentenceSemanticGraph().prettyPrint();
                dout.print("\n");
            }

            if (options.has("t")) {
                dout.print("Tokens: \n");
                dout.print("index \t word \t lemma \t POS \t NER");
                dout.print("\n");
                for (IndexedWord word: result.getSentenceSemanticGraph().vertexListSorted()) {
                    dout.print(word.index() + "\t");
                    dout.print(word.word() + "\t");
                    dout.print(word.lemma() + "\t");
                    dout.print(word.tag() + "\t");
                    dout.print(word.ner() + "\n");
                }
                dout.print("\n");
            }

            if (propositions.size() < 1) {
                dout.println("No extraction found.");
                dout.print("\n");
            } else {
                for (AnnotatedProposition aProp : result.getPropositions()) {
                    //dout.println(Utils.formatProposition(aProp));
                    dout.println(aProp.toString());
                }
                dout.print("\n");
            }
        }

        // clean up
        in.close();
        out.close();
    }

    /**
     * initializes and configures the option parser
     * @return a configured option parser
     */
    private static OptionParser initOptionParser() {
        OptionParser optionParser = new OptionParser();
        optionParser
                .accepts("f", "input file (if absent, MinIE reads from stdin)")
                .withOptionalArg()
                .describedAs("file")
                .ofType(String.class);
        optionParser
                .accepts("o", "output file (if absent, MinIE writes to stdout)")
                .withRequiredArg()
                .describedAs("file")
                .ofType(String.class);
        optionParser
                .accepts("m", "specification mode; allowed values: \"safe\", \"dictionary\", \"aggressive\", \"complete\"; defaults to \"safe\"")
                .withRequiredArg()
                .describedAs("mode")
                .ofType(String.class)
                .defaultsTo("safe");
        optionParser
                .accepts("dict", "path of the multi-word expression dictionaries (can be several paths separated by ';'); \"dictionary\" mode only")
                .withOptionalArg()
                .ofType(String.class)
                .withValuesSeparatedBy(';');
        optionParser
                .accepts("dict-overwrite", "if set, the default dictionary (multi-word expressions from WordNet and Wiktionary), will be overwritten, else new dictionaries will be appended")
                .withOptionalArg();
        optionParser
                .accepts("p", "print the dependency parse of the input sentence");
        optionParser
                .accepts("t", "print the tokens and their annotations (POS tags, lemmas, NERs, ... )");
        optionParser
                .accepts("ref-date", "reference date for the temporal annotations. Please enter the reference date in the following format: " +
                        "YYYY-MM-DD (by default, it's NONE, or XXXX-XX-XX")
                .withRequiredArg()
                .describedAs("ref-date")
                .ofType(String.class)
                .defaultsTo("XXXX-XX-XX");
        optionParser
                .accepts("h", "show help");
        return optionParser;
    }

    /**
     * returns input stream according to given options
     * @param options: option set for option parser
     * @return input stream
     */
    private static InputStream getInputStream(OptionSet options) {
        InputStream in = null;
        // check if input file was specified
        if (options.has("f")) {
            try {
                String filename = (String)options.valueOf("f");
                in = new FileInputStream(filename);
                logger.info("Reading from file " + filename);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        } else {
            // default to stdin
            in = System.in;
            logger.info("Reading from stdin");
        }
        assert in != null;
        return new DataInputStream(in);
    }

    /**
     * returns output stream according to given options
     * @param options: option set for option parser
     * @return output stream
     */
    private static OutputStream getOutputStream(OptionSet options) {
        OutputStream out = null;
        // check if output file was specified
        if (options.has("o")) {
            try {
                String filename = (String) options.valueOf("o");
                out = new FileOutputStream(filename);
                logger.info("Writing to file " + filename);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        } else {
            // default to stdout
            out = System.out;
            logger.info("Writing to stdout");
        }
        assert out != null;
        return new PrintStream(out);
    }
}
