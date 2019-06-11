package de.uni_mannheim.querykb.query.online;

import java.net.MalformedURLException;
import java.net.URL;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;

/**
 * @author Sven Hertling
 * @author Kiril Gashteovski
 */
public class OnlineVersion {
   
    public static void main(String[] args) throws MalformedURLException {
        //Or use our own dbpedia server BUT with a old version:  http://wifo5-20.informatik.uni-mannheim.de:8891/sparql
        String endpoint = "https://dbpedia.org/sparql";
        
        String obama = "https://en.wikipedia.org/wiki/Barack_Obama";
        String harvard = "https://en.wikipedia.org/wiki/Harvard_Law_School";
        String merkel = "https://en.wikipedia.org/wiki/Angela_Merkel";
        
        System.out.println(isConnected(endpoint, obama, harvard));
        System.out.println(isConnected(endpoint, obama, merkel));
        
        System.out.println(isConnectedFullWikiURL(endpoint, obama, harvard));
        System.out.println(isConnectedFullWikiURL(endpoint, obama, merkel));
        
        System.out.println("\nDONE!!!\n");
        //Genereally it is not taken care of:
        //- inverse triple??? urlb --> urla. not done yet
        //and to protect sparql injection, one can use ParameterizedSparqlString
    }
    
    public static boolean isConnected(String endpoint, String urlA, String urlB){
        
        String titleA = urlA.substring(urlA.lastIndexOf("/") + 1);
        String titleB = urlB.substring(urlB.lastIndexOf("/") + 1);

        Query q = QueryFactory.create(
                String.format("ASK { <http://dbpedia.org/resource/%s> ?p <http://dbpedia.org/resource/%s>. }",
                titleA, titleB));
        try (QueryExecution qexec = QueryExecutionFactory.createServiceRequest(endpoint, q)) {
            return qexec.execAsk();
        }
    }
    
    public static boolean isConnectedFullWikiURL(String endpoint, String urlA, String urlB) throws MalformedURLException{
        Query q = QueryFactory.create(
                String.format("ASK { ?s <http://xmlns.com/foaf/0.1/isPrimaryTopicOf> <%s>."
                        + "?o <http://xmlns.com/foaf/0.1/isPrimaryTopicOf> <%s>."
                        + "?s ?p ?o. }",
                changeProtocol(urlA), changeProtocol(urlB)));//Take care of protocol - no https but only http
        try (QueryExecution qexec = QueryExecutionFactory.createServiceRequest(endpoint, q)) {
            return qexec.execAsk();
        }
    }

    private static String changeProtocol(String url) throws MalformedURLException{
        URL oldUrl = new URL(url);
        return new URL("http", oldUrl.getHost(), oldUrl.getPort(), oldUrl.getFile()).toString();
    }
}