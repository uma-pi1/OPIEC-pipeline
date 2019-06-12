import io
import json
import pickle
import base64
import pdb
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import os
import copy 

def get_immediate_subdirectories(a_dir):
    return [name for name in os.listdir(a_dir)
            if os.path.isdir(os.path.join(a_dir, name))]

# Constants
AVRO_SCHEMA_FILE = "../../../avroschema/WikiArticleLinked.avsc"

# Enter correct dir here
home_dir = ".../enwiki-latest-wikiextractor/"
subdirs = get_immediate_subdirectories(home_dir)
for sub_dir in subdirs:
    dir_path = home_dir + sub_dir

    # Avro file
    AVRO_FILE = "../../../avro/wiki-files/" + "-".join(dir_path.split("/")[-2:]) + ".avro"
    wiki_schema = avro.schema.Parse(open(AVRO_SCHEMA_FILE, "rb").read().decode("utf-8"))
    writer = DataFileWriter(open(AVRO_FILE, "wb"), DatumWriter(), wiki_schema)
    print("writing file " + AVRO_FILE + " ....")

    sub_sub_dirs = get_immediate_subdirectories(home_dir + sub_dir)

    weird_case = 0
    for sub_sub_dir in sub_sub_dirs:    
        # From each file, write into the avro schema
        files = os.listdir(dir_path + "/" + sub_sub_dir)
        #pdb.set_trace()
        for f in files:
            file_path = dir_path + "/" + sub_sub_dir + "/" + f
            with io.open(file_path) as f:
                for l in f.readlines():
                    # Get article + links
                    wiki_article = json.loads(l)
                    internal_links = pickle.loads(base64.b64decode(wiki_article['internal_links'].encode('utf-8')))

                    # Create link objects
                    link_keys = []
                    for x in internal_links.keys():
                        link_keys.append(x)
                    links = []
                    try:
                        for link_k in link_keys:
                            link = {
                                "offset_begin_ind": link_k[0],
                                "offset_end_ind": link_k[1],
                                "phrase": internal_links[link_k][0],
                                "wiki_link": internal_links[link_k][1]
                            }
                            links.append(link)
                    except (IndexError):
                        weird_case = weird_case + 1
                        print("WEIRD CASE!\t" + "No: " + str(weird_case) + "\t" + str(link_k) + ": " + str(internal_links[link_k]))
                        link = {
                                "offset_begin_ind": link_k[0],
                                "offset_end_ind": link_k[1],
                                "phrase": internal_links[link_k][0],
                                "wiki_link": internal_links[link_k][0]
                        }
                        links.append(link)
                        #pdb.set_trace()
                    
                    # Remove title (information retained in wiki_article['title']) 
                    # and footer from text where categories are written (e.g. Category:Retail companies established in 1940)
                    paragraph_split = wiki_article['text'].split("\n\n")
                    clean_text = "\n\n".join(paragraph_split[1:])
                    
                    if ("Category:") in clean_text:
                        clean_text = clean_text.split("Category:")[0]

                    # Adjust the links offsets
                    links_offset_adjustment = len(paragraph_split[0]) + 2
                    final_links = []
                    for link in links:
                        link['offset_begin_ind'] -= links_offset_adjustment
                        link['offset_end_ind'] -= links_offset_adjustment
                        if link['offset_begin_ind'] < len(clean_text):
                            final_links.append(link)
                    
                    # Add link for the first sentence in case the spans match with the title
                    if (clean_text[0:len(wiki_article['title'])] == wiki_article['title']):
                        #add another link when done
                        link = {}
                        link['offset_begin_ind'] = 0
                        link['offset_end_ind'] = len(wiki_article['title'])
                        link['phrase'] = wiki_article['title']
                        link['wiki_link'] = wiki_article['title']
                        final_links.append(link)

                    # Populate article dictionary
                    article_dict = {
                        "title": wiki_article['title'], 
                        "id": wiki_article['id'],
                        "url": wiki_article['url'],
                        "text": clean_text,
                        "links": final_links
                    }

                    # Write to avro
                    writer.append(article_dict)

writer.close()

print("\nDONE!!!\n")
