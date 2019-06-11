package de.uni_mannheim.querykb.query.offline;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
/**
 * @author Sven Hertling
 * @author Kiril Gashteovski
 */

public class OfflineVersion {

    public static void main(String[] args) throws IOException {
        //download: 
        //https://wiki.dbpedia.org/downloads-2016-10
        //choose Infobox Properties	and/or Infobox Properties Mapped
        String path = "C:\\dev\\dbpedia\\2016_10\\infobox_properties_mapped_en.ttl";
        Map<String, Set<String>> triples = getTriples(path);
        
        String obama = "https://en.wikipedia.org/wiki/Barack_Obama";
        String harvard = "https://en.wikipedia.org/wiki/Harvard_Law_School";
        String merkel = "https://en.wikipedia.org/wiki/Angela_Merkel";

        System.out.println(isConnected(triples, obama, harvard));
        System.out.println(isConnected(triples, obama, merkel));
    }
    
    
    public static boolean isConnected(Map<String, Set<String>> triples, String urlA, String urlB){
        String titleA = urlA.substring(urlA.lastIndexOf("/") + 1);
        String titleB = urlB.substring(urlB.lastIndexOf("/") + 1);
        String dbpediaA = "http://dbpedia.org/resource/" + titleA;
        String dbpediaB = "http://dbpedia.org/resource/" + titleB;
        System.out.println(triples.getOrDefault(dbpediaA, new HashSet<>()));
        return triples.getOrDefault(dbpediaA, new HashSet<>()).contains(dbpediaB);
    }
    
    
    public static Map<String, Set<String>> getTriples(String filePath) throws IOException{
        Map<String, Set<String>> triples = new HashMap<>();
        try (FileInputStream is = new FileInputStream(filePath)) {
            NxParser nxp = new NxParser();   
            nxp.parse(is);
            for (Node[] nx : nxp)
            {
                if(nx[2] instanceof Resource == false){
                    continue;
                }                                
                Set<String> objects = triples.get(nx[0].getLabel());
                if(objects == null){
                    objects = new HashSet<>();
                    objects.add(nx[2].getLabel());
                    triples.put(nx[0].getLabel(), objects);
                }else{
                    objects.add(nx[2].getLabel());
                }
            }
        }
        return triples;
    }

}
