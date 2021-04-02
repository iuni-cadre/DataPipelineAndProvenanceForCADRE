from elasticsearch import Elasticsearch
from elasticsearch import helpers
import csv


es = Elasticsearch([{'host': 'iuni2.carbonate.uits.iu.edu','port': 9200}],
                   http_auth=('<username>', '<password>'))

if not es.ping():
    raise ValueError("Connection failed")



years = list(range(1997, 2021, 1))

for j in years: 

    search_body = {"size": 10,
    			   "query": {
          				"match": {
            				"doc.pub_info._pubyear": j          
        }
      }
    }
     
    
    resp = helpers.scan(
    	es, 
    	query = search_body,
    	scroll = '5m', 
    	size = 10, 
    	raise_on_error = False
    )
    
    
    export = []
    
        
    for num, doc in enumerate(resp): 
            
        one_paper = dict(doc)
    
        one_rec = one_paper['_source']['doc']
        
        authors_full_name = str()
        authors_first_name = str()
        authors_last_name = str()
        authors_suffix = str()
        authors_initials = str()
        authors_display_name = str()
        authors_wos_name = str()
    
        authors_email = str()
    
        #Select Authors
        for i in [*range(0,len(one_rec['name'][0]), 1)]:
            try: 
                authors_full_name = authors_full_name       + '| ' + one_rec['name'][0][i]['full_name']
            except KeyError: 
                authors_full_name = authors_full_name
            try: 
                authors_first_name = authors_first_name     + '| ' + one_rec['name'][0][i]['first_name']
            except KeyError:
                authors_first_name = authors_first_name
            try: 
                authors_last_name    = authors_last_name    + '| ' + one_rec['name'][0][i]['last_name']
            except KeyError:
                authors_last_name = authors_last_name
            try: 
                authors_suffix  = authors_suffix       + '| ' + one_rec['name'][0][i]['suffix']
            except KeyError:
                authors_suffix = authors_suffix
            try: 
                authors_display_name = authors_display_name + '| ' + one_rec['name'][0][i]['display_name']
            except KeyError: 
                authors_display_name = authors_display_name
            try: 
                authors_wos_name     = authors_wos_name     + '| ' + one_rec['name'][0][i]['wos_standard']
            except KeyError: 
                authors_wos_name = authors_wos_name
            try: 
                authors_email = authors_email  + '| ' + one_rec['name'][0][i]['email_addr']
            except KeyError: 
                authors_email = authors_email
        
        
        authors_id_orcid = str()
        authors_id_dais = str()
        authors_id_research = str()
    
    #Select ids
        try:
            for i in [*range(0,len(one_rec['contributor'][0]), 1)]:
                try:
                    authors_id_orcid = authors_id_orcid  + '| ' + one_rec['contributor'][0][i]['name']['_orcid_id']
                except KeyError: 
                    authors_id_orcid = authors_id_orcid
                try: 
                    authors_id_dais = authors_id_dais  + '| ' + one_rec['contributor'][0][i]['name']['_dais_id']
                except KeyError: 
                    authors_id_dais = authors_id_dais
                try: 
                    authors_id_research = authors_id_research  + '| ' + one_rec['contributor'][0][i]['name']['_r_id']
                except KeyError: 
                    authors_id_research = authors_id_research
        except KeyError:
            pass
    
    
    
    
        references = str()
    
    #Select References
        try:
            for i in [*range(0, len(one_rec['reference'][0]), 1)]:
                try: 
                    references = references + '| ' + one_rec['reference'][0][i]['uid']
                except: 
                    references = references
        except KeyError:
            pass
    
    #Select other values      
        try:
            issue = one_rec['pub_info'][0]['_issue']
        except:
            issue = ''
        try:
            abstract = one_rec['abstract_text'][0]['p'] 
        except: 
            abstract = ''
        
        try: 
            pages = (one_rec['pub_info'][0]['page']['_begin'] +'-'+ one_rec['pub_info'][0]['page']['_end'])
        except:
            pages = ''
            
        
    #Select alt ids
        try:
            ident = one_rec['identifier'][0]
                
            try:
                issn = next(item["_value"] for item in ident if item["_type"] == "issn")
            except:
                issn = ""
            try:
                doi  = next(item["_value"] for item in ident if item["_type"] == "doi")
            except: 
                doi = ''
        except:
            doi = ''
            issn = ''
    
    
    #Select journal names
            
        titles = one_rec['titles'][0]['title']
        
        try: 
            journal_name = next(item["_VALUE"] for item in titles if item["_type"] == "source")
        except: 
            journal_name = ''
            
        try: 
            journal_abbrev = next(item["_VALUE"] for item in titles if item["_type"] == "source_abbrev")
        except: 
            journal_abbrev = ''
            
        try: 
            journal_iso = next(item["_VALUE"] for item in titles if item["_type"] == "abbrev_iso")
        except: 
            journal_iso = ''
            
        try: 
            title = next(item["_VALUE"] for item in titles if item["_type"] == "item")
        except:
            title = '' 
            
    
        output = {"UID" : one_rec['UID'],
                  "year": one_rec['pub_info'][0]['_pubyear'],
                  "number": '',
                  "issue" : issue, 
                  "pages": pages, 
                  "authors_full_name": authors_full_name[2:],
                  "authors_id_orcid": authors_id_orcid[2:],
                  "authors_id_dais": authors_id_dais[2:],
                  "authors_id_research": authors_id_research[2:],
                  "authors_prefix": '',
                  "authors_first_name": authors_first_name[2:],
                  "authors_middle_name": '', 
                  "authors_last_name": authors_last_name[2:],
                  "authors_suffix": authors_suffix[2:], 
                  "authors_initials": '',
                  "authors_display_name": authors_display_name[2:],
                  "authors_wos_name": authors_wos_name[2:], 
                  "authors_id_lang":'',
                  "authors_email": authors_email[2:], 
                  "references": references[2:],
                  "issn": issn,
                  "doi": doi,
                  "title": title,
                  "journal_name":journal_name,
                  "journal_abbrev": journal_abbrev,
                  "journal_iso": journal_iso,
                  "abstract_paragraph": abstract  
                  }
    
        #print(output)
    
        export.append(output)
    
    
    file_name = '/N/dc2/scratch/maahutch/el_output/'+str(j)+'.csv'
    
    with open(file_name, 'w') as csv_file:
        writer = csv.DictWriter(csv_file, output.keys())
        for i in export: 
            writer.writerow(i)
