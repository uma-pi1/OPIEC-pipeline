/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.querykb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Sven Hertling
 * @author Kiril Gashteovski
 */
public class KBQueryDB implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(KBQueryDB.class);
    private static final String DBpediaPrefix = "http://dbpedia.org/resource/";
    private static final int DBpediaPrefixLength = DBpediaPrefix.length();
    private static final String YagoPrefix = "http://yago-knowledge.org/resource/";

    private Connection con;

    public KBQueryDB(File pathToDB) {
        try {
            Class.forName("org.sqlite.JDBC");
            con = DriverManager.getConnection("jdbc:sqlite:" + pathToDB.getCanonicalPath());
        } catch (Exception e) {
            logger.error("Can not create connection to database file.", e);
            return;
        }
        createSchema();
        logger.info("Opened database successfully");
    }

    private void createSchema(){
        try (Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS labels (url text NOT NULL, label text NOT NULL);");
            stmt.execute("CREATE INDEX IF NOT EXISTS search_label ON labels (label);");

            stmt.execute("CREATE TABLE IF NOT EXISTS redirects (url text NOT NULL PRIMARY KEY, redirected text NOT NULL);");

            stmt.execute("CREATE TABLE IF NOT EXISTS disambiguation (url text NOT NULL, disambig text NOT NULL);");
            stmt.execute("CREATE INDEX IF NOT EXISTS search_disambiguation ON disambiguation (url);");

            stmt.execute("CREATE TABLE IF NOT EXISTS triples (subject text NOT NULL, predicate text NOT NULL, object text NOT NULL);");
            stmt.execute("CREATE INDEX IF NOT EXISTS search_subject_triple ON triples (subject);");
            stmt.execute("CREATE INDEX IF NOT EXISTS search_object_triple ON triples (object);");

            stmt.execute("CREATE TABLE IF NOT EXISTS literals (subject text NOT NULL, predicate text NOT NULL, object text NOT NULL);");
            stmt.execute("CREATE INDEX IF NOT EXISTS search_subject_literal ON literals (subject);");
            stmt.execute("CREATE INDEX IF NOT EXISTS search_object_literal ON literals (object);");

            //stmt.execute("CREATE TABLE IF NOT EXISTS triples (subject text NOT NULL, predicate text NOT NULL, object text NOT NULL);");
        }catch (SQLException e) {
            logger.error("SQL error", e);
        }
    }

    private String processTextForRetrieval(String text){
        return text.toLowerCase().trim();
    }

    public void addYagoTripleFile(String path) throws SQLException {
        addYagoTripleFile(Integer.MAX_VALUE, path);
    }

    public void addYagoTripleFile(int max_triples_per_file, String path) throws SQLException {
        logger.info("Parse yago file {}", path);
        //or use 7-zip file
        int i = 0;
        PreparedStatement triplePs = con.prepareStatement("INSERT INTO triples (subject, predicate, object) VALUES (?, ?, ?)");
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))) {
            String entireLine = "";
            while ((entireLine = br.readLine()) != null) {
                String[] line = entireLine.split("\t");
                if (line.length != 4) {
                    logger.error("Wrong number of arguments: Line: {}", entireLine);
                    continue;
                }
                String id = getPageTitleFromYago(line[0]);
                String subject = getPageTitleFromYago(line[1]);
                String predicate = getPageTitleFromYago(line[2]);
                String object = getPageTitleFromYago(line[3]);

                if (subject != null && predicate != null && object != null) {
                    triplePs.setString(1, subject);
                    triplePs.setString(2, YagoPrefix + predicate);
                    triplePs.setString(3, object);
                    triplePs.addBatch();
                }

                if(i % 1000 == 0){
                    triplePs.executeBatch();
                    logger.info("Send {} batch", i);
                }
                if (i > max_triples_per_file) {
                    logger.info("Break parsing at {} triples", i);
                    break;
                }
                i++;
            }
        } catch (IOException ex) {
            logger.warn("Couldn't read file", ex);
        }
        triplePs.executeBatch();
        triplePs.close();
        logger.info("Finished parsing file {}", path);
    }

    private String getPageTitleFromYago(String yagoResource) {
        if (yagoResource.startsWith("<") && yagoResource.endsWith(">")) {
            return yagoResource.substring(1, yagoResource.length() - 1);
        }
        return null;
    }

    public void addDbpediaLabelFile(String path) throws SQLException {
        addDbpediaLabelFile(Integer.MAX_VALUE, path);
    }

    public void addDbpediaLabelFile(int max_triples_per_file, String path) throws SQLException {
        logger.info("Parse dbpedia label file {}", path);
        InputStream fileStream = null;
        try {
            fileStream = this.getClass().getResource(path).openStream();
        } catch (FileNotFoundException ex) {
            logger.warn("File is not found.", ex);
            return;
        } catch (IOException e) {
            logger.warn("IOException. ", e);
        }

        PreparedStatement ps = con.prepareStatement("INSERT INTO labels (url, label) VALUES (?, ?)");
        try (InputStream in = (path.endsWith(".bz2") ? new BZip2CompressorInputStream(fileStream) : fileStream)) {
            NxParser nxp = new NxParser();
            nxp.parse(in);
            int i = 0;
            for (Node[] nx : nxp) {
                String subject = nx[0].getLabel();
                String object = nx[2].getLabel();

                if (subject.startsWith(DBpediaPrefix) == false) {
                    continue;
                }
                subject = subject.substring(DBpediaPrefixLength); //remove prefix
                object = processTextForRetrieval(object);//preprocess label

                ps.setString(1, subject);
                ps.setString(2, object);
                ps.addBatch();
                if(i % 1000 == 0){
                    ps.executeBatch();
                    logger.info("Send {} batch", i);
                }
                if (i > max_triples_per_file) {
                    logger.info("Break parsing at {} triples", i);
                    break;
                }
                i++;
            }
        } catch (IOException ex) {
            logger.warn("Couldn't read file", ex);
        }
        logger.info("Finished parsing file {}", path);
        ps.executeBatch(); // insert remaining records
        ps.close();
    }

    public void addDbpediaRedirectFile(String path) throws SQLException {
        addDbpediaLabelFile(Integer.MAX_VALUE, path);
    }

    public void addDbpediaDisambiguationFile(String path) throws SQLException {
        addDbpediaDisambiguationFile(Integer.MAX_VALUE, path);
    }

    public void addDbpediaDisambiguationFile(int max_triples_per_file, String path) throws SQLException {
        logger.info("Parse dbpedia disambiguation file {}", path);
        InputStream fileStream = null;
        try {
            fileStream = this.getClass().getResource(path).openStream();
        } catch (FileNotFoundException ex) {
            logger.warn("File is not found.", ex);
            return;
        } catch (IOException e) {
            logger.warn("IOException. ", e);
        }

        PreparedStatement ps = con.prepareStatement("INSERT INTO disambiguation (url, disambig) VALUES (?, ?)");
        try (InputStream in = (path.endsWith(".bz2") ? new BZip2CompressorInputStream(fileStream) : fileStream)) {
            NxParser nxp = new NxParser();
            nxp.parse(in);
            int i = 0;
            for (Node[] nx : nxp) {
                if (nx[2] instanceof Resource && nx[0] instanceof Resource) {
                    String subject = nx[0].getLabel();
                    String object = nx[2].getLabel();
                    if (subject.startsWith(DBpediaPrefix) && object.startsWith(DBpediaPrefix)) {
                        subject = subject.substring(DBpediaPrefixLength); //remove prefix
                        object = object.substring(DBpediaPrefixLength);

                        ps.setString(1, subject);
                        ps.setString(2, object);
                        ps.addBatch();
                    }
                }
                if(i % 1000 == 0){
                    ps.executeBatch();
                    logger.info("Send {} batch", i);
                }
                if (i > max_triples_per_file) {
                    logger.info("Break parsing at {} triples", i);
                    break;
                }
                i++;
            }
        } catch (IOException ex) {
            logger.warn("Couldn't read file", ex);
        }
        ps.executeBatch();
        ps.close();
        logger.info("Finished parsing file {}", path);
    }

    public void addDbpediaTripleFiles(String... paths) throws SQLException {
        for (String path : paths) {
            addDbpediaTripleFile(path);
        }
    }

    public void addDbpediaTripleFile(String path) throws SQLException {
        addDbpediaTripleFile(Integer.MAX_VALUE, path);
    }

    public void addDbpediaTripleFile(int max_triples_per_file, String path) throws SQLException {
        logger.info("Parse dbpedia file {}", path);
        InputStream fileStream = null;
        try {
            fileStream = this.getClass().getResource(path).openStream();
        } catch (FileNotFoundException ex) {
            logger.warn("File is not found.", ex);
            return;
        } catch (IOException e) {
            logger.warn("IOException. ", e);
        }

        PreparedStatement triplePs = con.prepareStatement("INSERT INTO triples (subject, predicate, object) VALUES (?, ?, ?)");
        PreparedStatement literalPs = con.prepareStatement("INSERT INTO literals (subject, predicate, object) VALUES (?, ?, ?)");
        try (InputStream in = (path.endsWith(".bz2") ? new BZip2CompressorInputStream(fileStream) : fileStream)) {
            NxParser nxp = new NxParser();
            nxp.parse(in);
            int i = 0;
            for (Node[] nx : nxp) {
                String subject = nx[0].getLabel();
                String predicate = nx[1].getLabel();
                String object = nx[2].getLabel();

                if (nx[2] instanceof Resource) {
                    //just use resource and not uris in different namespace
                    if (subject.startsWith(DBpediaPrefix) && object.startsWith(DBpediaPrefix)) {
                        //Store only title and not full uri
                        subject = subject.substring(DBpediaPrefixLength); //remove prefix
                        object = object.substring(DBpediaPrefixLength);
                        //add triple to map
                        triplePs.setString(1, subject);
                        triplePs.setString(2, predicate);
                        triplePs.setString(3, object);
                        triplePs.addBatch();
                    }
                } else if (nx[2] instanceof Literal) {
                    if (subject.startsWith(DBpediaPrefix)){
                        subject = subject.substring(DBpediaPrefixLength); //remove prefix
                        literalPs.setString(1, subject);
                        literalPs.setString(2, predicate);
                        literalPs.setString(3, object);
                        literalPs.addBatch();
                    }
                }

                if(i % 1000 == 0){
                    triplePs.executeBatch();
                    literalPs.executeBatch();
                    logger.info("Send {} batch", i);
                }
                if (i > max_triples_per_file) {
                    logger.info("Break parsing at {} triples", i);
                    break;
                }
                i++;
            }
        } catch (IOException ex) {
            logger.warn("Couldn't read file", ex);
        }
        logger.info("Finished parsing file {}", path);
        literalPs.executeBatch();
        literalPs.close();
        triplePs.executeBatch();
        triplePs.close();
    }


    public Set<String> getConnectingPropertiesGivenPageTitle(String pageTitleSource, String pageTitleTarget) throws SQLException {
        //TODO: check for redirects and disambiguations
        PreparedStatement triplePs = con.prepareStatement("SELECT predicate FROM triples WHERE subject = ? AND object = ?");
        triplePs.setString(1, pageTitleSource);
        triplePs.setString(2, pageTitleTarget);
        ResultSet rs = triplePs.executeQuery();

        Set<String> result = new HashSet<>();
        while (rs.next()) {
            result.add(rs.getString("predicate"));
        }
        return result;
    }

    public Set<String> getDisambiguationsOrSingleResource(String text) throws SQLException {
        //look up lowercased and stripped string in labels file
        //check afterwards if it is a disambiguation file
        //preprocess text as in index time (sse addDbpediaLabelFile):


        String lookup = this.processTextForRetrieval(text);

        PreparedStatement triplePs = con.prepareStatement(
                "WITH redirected_urls AS (\n" +
                        "SELECT ifnull(redirects.redirected, tmp.url) AS urls\n" +
                        "FROM (SELECT url FROM labels WHERE label = ?) as tmp LEFT JOIN redirects ON tmp.url = redirects.url\n" +
                        ")\n" +
                        "\n" +
                        "SELECT ifnull(disambiguation.disambig, redirected_urls.urls) AS result  \n" +
                        "FROM redirected_urls LEFT JOIN disambiguation ON redirected_urls.urls = disambiguation.url");

        triplePs.setString(1, lookup);
        ResultSet rs = triplePs.executeQuery();

        Set<String> result = new HashSet<>();
        while (rs.next()) {
            result.add(rs.getString("result"));
        }
        return result;
    }

    public static void main(String args[]) throws IOException, SQLException {
        String KG_BASE_DIR = "C:\\dev\\dbpedia\\2016_10\\";//"/home/shertlin-tmp/dbpedia/2016_10/"

        KBQueryDB querydb = new KBQueryDB(new File("./test.db"));
        //querydb.addDbpediaLabelFile(100, KG_BASE_DIR + "labels_en.ttl.bz2");
        //querydb.addDbpediaRedirectFile(100, KG_BASE_DIR + "transitive_redirects_en.ttl.bz2");
        //querydb.addDbpediaTripleFiles(100, KG_BASE_DIR + "infobox_properties_mapped_en.ttl.bz2");
        //querydb.addDbpediaDisambiguationFile(100, KG_BASE_DIR + "disambiguations_en.ttl.bz2");

        //System.out.println(querydb.getConnectingPropertiesGivenPageTitle("Alain_Connes", "Jacques_Dixmier"));
        //System.out.println(querydb.getSubjectLiteralRelations("Abraham_Lincoln", "1865-04-15"));
        System.out.println(querydb.getDisambiguationsOrSingleResource("Alien"));

        /*
        KBOfflineQuery tmp = new KBOfflineQuery();
            tmp.addDbpediaLabelFile(maxTriplePerFile, KG_BASE_DIR + "labels_en.ttl.bz2");
            tmp.addDbpediaDisambiguationFile(maxTriplePerFile, KG_BASE_DIR + "disambiguations_en.ttl.bz2");
            tmp.addDbpediaRedirectFile(maxTriplePerFile, KG_BASE_DIR + "transitive_redirects_en.ttl.bz2");
            tmp.addDbpediaTripleFiles(maxTriplePerFile,
                   KG_BASE_DIR + "mappingbased_objects_en.ttl.bz2",
                   KG_BASE_DIR + "infobox_properties_mapped_en.ttl.bz2",
                   KG_BASE_DIR + "infobox_properties_en.ttl.bz2");
            return tmp;
        */
    }
}
