## Master Files Explained

 * ##### all_id_mapping.csv
   This file contains the mapping between company URL, firm_id, NAICS code and the source from which NAICS code was 
   obtained. This file is created by `generate_all_id_mapping.py` script. And this file has the following header:
   
    ```commandline
    company    firm_id    naics    obtained_from   
    ```
    
 * ##### company_firmid_dict.txt
    This file has the same content as in the above file but in a dictionary format which can be easily used in the 
    python scripts. So the key is company_url (string) in normalized form and the value is a tuple of 3 values. 
    (firm_id, naics_code, obtained_from). firm_id is int type, naics_code and obtained_from are string types. This file
    is created by `generate_master_indexes_1.py` script.  
    
 * ##### firm_id_naics_mapping_dict.txt
    This file has the mapping between firm_id to NAICS code, in a dictionary format. So the key is firm_id and the value
    is NAICS code. Note: It doesn't have firm_ids which don't have corresponding NAICS code.
    
 * ##### master_id_to_firm_id.txt
    This file contains the mapping between master_id and firm_id for each unique company url. master_id can be found in 
    `URL_id_file.zip`. This file is created by `generate_master_files_1.py` script. And this file has following header: 
    
    ```commandline
    company    master_id    firm_id   
    ```
    
 * ##### masterId_firmId_mapping_dict.txt
    This file has the same content as the `master_id_to_firm_id.txt` but in a dictionary format. In which, key is company
    url in string type and the value is a tuple (master_id, firm_id). Both master_id and firm_id are int types. This file
    is created by `generate_master_indexes_1.py` script.
    
 * ##### new_normalized_naics_code.txt
    This file has the same content as the `company_firmid_dict.txt` in the same format. But the difference is that, it
    has only those company urls as the keys which has NAICS code. This file is created by `generate_master_files.py` 
    script.
    
 * ##### public_companies_firmid_gvkeys_yearwise
    This folder has a list of csv files according to the years with the following header:
    
    ```commandline
    company    firm_id    gvkey    profit_assets    profit_sales    stock_return    valuation
    ```
    This is only for public companies. This folder is created by `generate_public_firms_1.py` script.
 
 * ##### yearwise_public_companies_list
    This folder has a list of text files according to the years. Each file contains the list of public companies for 
    which we have web data. These files are used as input for the Doc2Vec training. This folder is created by 
    `generate_public_firms.py` script.
    
 * ##### yearwise_companies_list
    This folder has a list of text files according to the years. Each file contains the list of private companies for 
    which we have web data. These files are used as input for the Doc2Vec training. This folder is been created by 
    `generate_master_files.py` script.
    