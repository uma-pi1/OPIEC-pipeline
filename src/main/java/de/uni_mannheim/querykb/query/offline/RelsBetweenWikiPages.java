package de.uni_mannheim.querykb.query.offline;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.nio.charset.StandardCharsets;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Sven Hertling
 * @author Kiril Gashteovski
 */
public class RelsBetweenWikiPages {

    private static final Logger logger = LoggerFactory.getLogger(RelsBetweenWikiPages.class);
    private static final String DBpediaPrefix = "http://dbpedia.org/resource/";
    private static final int DBpediaPrefixLength = DBpediaPrefix.length();
    private static final String YagoPrefix = "http://yago-knowledge.org/resource/";

    private Map<String, Map<String, Set<String>>> triples;
    private int max_triples_per_file = Integer.MAX_VALUE;

    public RelsBetweenWikiPages() {
        this.triples = new HashMap<>();
    }

    private void addYagoFile(String path) {
        logger.info("Parse yago file {}", path);
        //or use 7-zip file
        int i = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))) {
            String entireLine = "";
            while ((entireLine = br.readLine()) != null) {
                String[] line = entireLine.split("\t");
                if(line.length != 4){
                    logger.error("Wrong number of arguments: Line: {}", entireLine);
                    continue;
                }
                //String id = getPageTitleFromYago(line[0]);
                String subject = getPageTitleFromYago(line[1]);
                String predicate = getPageTitleFromYago(line[2]);
                String object = getPageTitleFromYago(line[3]);

                if(subject != null && predicate != null && object != null)
                    addTripleToMap(subject, YagoPrefix + predicate, object);

                if (i % 100000 == 0) {
                    logger.info("Parsed {} triples", i);
                    if (i > max_triples_per_file) {
                        logger.info("Break parsing at {} triples", i);
                        break;
                    }
                }
                i++;
            }
        } catch (IOException ex) {
            logger.warn("Couldn't read file", ex);
        }
        logger.info("Finished parsing file {}", path);
    }

    private String getPageTitleFromYago(String yagoResource){
        if(yagoResource.startsWith("<") && yagoResource.endsWith(">"))
            return yagoResource.substring(1, yagoResource.length() - 1);
        return null;
    }


    private void addDbpediaFile(String path) {
        logger.info("Parse dbpedia file {}", path);
        InputStream fileStream;
        try {
            fileStream = new BufferedInputStream(new FileInputStream(path));
        } catch (FileNotFoundException ex) {
            logger.warn("File is not found.", ex);
            return;
        }

        try (InputStream in = (path.endsWith(".bz2") ? new BZip2CompressorInputStream(fileStream) : fileStream)) {
            NxParser nxp = new NxParser();
            nxp.parse(in);
            int i = 0;
            for (Node[] nx : nxp) {
                if (nx[2] instanceof Resource == false) {
                    continue;
                }
                String subject = nx[0].getLabel();
                String predicate = nx[1].getLabel();
                String object = nx[2].getLabel();
                if (subject.startsWith(DBpediaPrefix) == false || object.startsWith(DBpediaPrefix) == false) {
                    continue; // just use resource and not uris in different namespace
                }
                //Store only title and not full uri
                subject = subject.substring(DBpediaPrefixLength); //remove prefix
                object = object.substring(DBpediaPrefixLength);

                //add triple to map
                addTripleToMap(subject, predicate, object);
                if (i % 100000 == 0) {
                    logger.info("Parsed {} triples", i);
                    if (i > max_triples_per_file) {
                        logger.info("Break parsing at {} triples", i);
                        break;
                    }
                }
                i++;
            }
        } catch (IOException ex) {
            logger.warn("Couldn't read file", ex);
        }
        logger.info("Finished parsing file {}", path);
    }

    private void addTripleToMap(String subject, String predicate, String object) {
        Map<String, Set<String>> object2Predicate = this.triples.get(subject);
        if (object2Predicate == null) {
            object2Predicate = new HashMap<>();
            object2Predicate.put(object, new HashSet(Arrays.asList(predicate)));
            this.triples.put(subject, object2Predicate);
        } else {
            Set<String> predicates = object2Predicate.get(object);
            if (predicates == null) {
                predicates = new HashSet<>(Arrays.asList(predicate));
                object2Predicate.put(object, predicates);
            } else {
                predicates.add(predicate);
            }
        }
    }

    public Set<String> getConnectingPropertiesGivenPageTitle(String pageTitleSource, String pageTitleTarget) {
        Map<String, Set<String>> object2Predicate = this.triples.get(pageTitleSource);
        if (object2Predicate != null) {
            Set<String> predicates = object2Predicate.get(pageTitleTarget);
            if (predicates != null) {
                return predicates;
            }
        }
        return new HashSet<>();
    }

    public static RelsBetweenWikiPages createDbpediaFromFiles(int maxTriplesPerFile, String... filePaths) {
        RelsBetweenWikiPages kboq = new RelsBetweenWikiPages();
        kboq.max_triples_per_file = maxTriplesPerFile;
        for (String path: filePaths) {
            kboq.addDbpediaFile(path);
        }
        return kboq;
    }

    public static RelsBetweenWikiPages createYagoFromFiles(int maxTriplesPerFile, String... filePaths) {
        RelsBetweenWikiPages kboq = new RelsBetweenWikiPages();
        kboq.max_triples_per_file = maxTriplesPerFile;
        for (String path: filePaths) {
            kboq.addYagoFile(path);
        }
        return kboq;
    }
}
