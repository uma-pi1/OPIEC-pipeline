package de.uni_mannheim.utils.minie;

import de.uni_mannheim.constant.NE_TYPE;
import edu.stanford.nlp.ling.IndexedWord;
import joptsimple.OptionSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import de.uni_mannheim.minie.MinIE;
import de.uni_mannheim.utils.Dictionary;

/**
 * Helper class for MinIE
 *
 * @author Martin Achenbach
 * @author Kiril Gashteovski
 */
public class Utils {
    /** MinIE default dictionaries **/
    public static String [] DEFAULT_DICTIONARIES = new String [] {"/minie-resources/wn-mwe.txt", 
                                                                  "/minie-resources/wiktionary-mw-titles.txt"};

    /**
     * parses a string to a MinIE mode
     * @param s: string to parse
     * @return MinIE mode
     */
    public static MinIE.Mode getMode(String s) {
        MinIE.Mode mode;
        if (s.equalsIgnoreCase("aggressive")) {
            mode = MinIE.Mode.AGGRESSIVE;
        } else if (s.equalsIgnoreCase("dictionary")) {
            mode = MinIE.Mode.DICTIONARY;
        } else if (s.equalsIgnoreCase("complete")) {
            mode = MinIE.Mode.COMPLETE;
        } else {
            mode = MinIE.Mode.SAFE;
        }
        return mode;
    }

    /**
     * load a dictionary from a given location in the option set
     * @param options: option set to read the locations from
     * @return a dictionary read from the specified locations
     * @throws IOException
     */
    public static Dictionary loadDictionary(OptionSet options) throws IOException {
        Dictionary collocationDictionary = null;
        ArrayList<String> filenames = new ArrayList<>();
        if (!options.has("dict-overwrite")) {
            // if the overwrite option is not set, add the default dictionaries
            filenames.addAll(Arrays.asList(DEFAULT_DICTIONARIES));
        }
        if (options.has("dict")) {
            filenames.addAll((Collection<? extends String>) options.valuesOf("dict"));
        }
        String[] filenamesArray = Arrays.copyOf(filenames.toArray(), filenames.size(), String[].class);
        //logger.info("Loading dictionaries from " + Arrays.toString(filenamesArray));
        collocationDictionary = new Dictionary(filenamesArray);
        //logger.info("Finished loading dictionaries");
        return collocationDictionary;
    }

    /**
     * Given the subject's head word and the object's head word, this function checks whether the conditions are met
     * for giving the "be" relation (e.g. "PERSON" "be" "ORGANIZATION" gives false)
     * @param subjHead head word of the subject
     * @param objHead head word of the object
     * @return true: if conditions are met; false otherwise
     */
    public static boolean argsNERCheckForBeRel(IndexedWord subjHead, IndexedWord objHead) {
        // This happens very rarely
        if (subjHead == null || objHead == null) {
            return false;
        }

        if (!subjHead.ner().equals(NE_TYPE.NO_NER) && !objHead.ner().equals(NE_TYPE.NO_NER)) {
                // "No question the athletic director , Bob Mulcahy , was correct in his assessment that Schiano , a New Jersey native with no head-coaching experience , was the right man for a major construction job ."
                //  -1502   "Schiano"       "is New Jersey native with"     "QUANT_O_1 head-coaching experience"             Quantifiers:O_1=no
                // " The Kurdistan Workers ' Party and its aliases , the Kurdistan Freedom and Democracy Congress and the Kurdistan People 's Congress , are terrorist organizations and have been designated as such under U.S. law , '' Mr. Bremer said . ''
                //  -1675   "aliases"       "is"    "Kurdistan Freedom"
                //  -1675   "aliases"       "is"    "Democracy Congress"
                //  -1675   "aliases"       "is"    "Kurdistan People 's Congress"
                // "Then there was Marie Moore , a 52-year-old who had become sexually enslaved by Tony , a 14-year-old , and who had tortured and murdered Belinda Weeks , another teen-ager ."
                // -2098   "Tony"  "is"    "14-year-old"
                // -2098   "Marie Moore"   "is"    "52-year-old" (DURATION)
                // Why here: "The letter , signed by Chief Robert F. Meyer and the department president , Edward T. Russell , said the town used to have firefighters who were '' commuters , executives , professionals , artists and salespersons '' living in town , and the department wants more of the same kind of resident volunteers ."
                //  -2865   "Edward T. Russell"     "is"    "department president"
                // "The Sandinistas are aware of our problems and want to take advantage of them , '' said Alfredo Cesar , one of the five directors of the Nicaraguan Resistance who attended the Managua meeting and the meeting with Mr. Shultz Wednesday ."
                // -3339   "Alfredo Cesar" "is"    "QUANT_O_1 of QUANT_O_2 directors of Nicaraguan Resistance"              Quantifiers:O_1=oneO_2=five
                //A new home for Stuyvesant High School , one of New York 's most prestigious public academic high schools , is being built just north of the World Financial Center .
                // -3838   "Stuyvesant High School"        "is QUANT_O_1 of"       "New York 's most prestigious public academic high schools"              Quantifiers:O_1=one
                // Antarctica , one of the last great tourism frontiers , is becoming more accessible with the advent of twice-weekly commercial flights from Argentina .
                // -4003   "Antarctica"    "is QUANT_O_1 of"       "last great tourism frontiers"           Quantifiers:O_1=one
                // -4643   "Kabab Cafe"    "is"    "QUANT_O_1 of first Egyptian businesses to open in area"                 Quantifiers:O_1=one
                //  We are living on credit , '' said Yusef Shawki Taher , 61 , a deeply tanned , distinguished-looking fisherman who lives down the coast in Tyre , in the Bas refugee camp . ''
                // -4658   "Yusef Shawki Taher in Bas refugee camp"        "is"    "QUANT_O_1"              Quantifiers:O_1=61
                // Although Jean-Jacques passes the inspection , Mathias finds himself taken at gunpoint to the customs car , where he is grilled by a sinister double-agent type named Bleicher -LRB- Jean-Louis Richard -RRB- .
                //  -4705   "Bleicher"      "is"    "Jean-Louis Richard"
                //  -4705   "Jean-Louis Richard"    "is"    "Bleicher"
                //  Glavine and his wife , Christine , are flying in from Atlanta , and they are eminently qualified to speak about their successful transition from the South to the Northeast .
                // -5205   "Christine"     "is"    "wife" WHY??? "wife" is NO_NER
                // Peg and Henry were strong supporters of the Buffalo Bill Historical Center -LRB- BBHC -RRB- in Wyoming .
                //  -5410   "Buffalo Bill Historical Center"        "is"    "BBHC"
                //  Artisans like the Aztec and Cora silversmith John Sevilla , 59 , make even more by selling their crafts .
                //  -7274   "Aztec John Sevilla"    "is"    "QUANT_O_1"              Quantifiers:O_1=59
                //  -7274   "Cora John Sevilla"     "is"    "QUANT_O_1"              Quantifiers:O_1=59
                // `` They sell the skin in Delhi to smugglers and traders , and then it goes to Nepal and then to South Asia , especially Singapore and Hong Kong , '' said the wildlife expert , Fateh Singh Rathore .
                // "Singapore"	"is"	"South Asia"
                //This month , six former chairmen of the Republican National Committee , including Senator Bob Dole and Haley Barbour , sent a letter to Mr. Bush urging him to support an expanded guest worker program and a limited legalization plan . ''
                // 6958	"Bob Dole"	"is"	"Republican National Committee"
                // TODO: check if NUMBER qualifies here too (and DURATION)
            if (!objHead.ner().equals(NE_TYPE.MISC)) {
                return false;
            }
        }

        return true;
    }
}
