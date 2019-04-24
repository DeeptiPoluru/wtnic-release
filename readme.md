#WTNIC
#### Web Text-based Network Industry Classification  

Capturing Organizational Form, Competition, and Industry Change through Text-mining of Private and Public Firm Webpages  

### Getting Started

Currently we have web crawled data for public and private firms. For each firm, we have stored words in order in 
text files. So you will find ordered bag of words for each firm sorted according to years. We have public companies' data
from 1996-2016, and private companies' data from 1994-2017.  


All the source code for this system is written in Python. So all the requirements for modules are listed in 
```requirements.txt``` file. So create a virtual environment and install all these packages in that environment.  

##### For Example
```
virtualenv wtnic_venv
source wtnic_venv/bin/activate
pip install -r requeriments.txt
```
To deactivate the virtual environment:
```
deactivate
```

##Workflow with Document embedding using Doc2Vec  

All the python scripts for this particular exercise can be found in  ```automation/doc2vec_sripts/``` directory. And the 
master files for mapping between different IDs can be found in ```automation/master_files``` directory.
 * ###Training
    According to our experiments, we have finalized to train Doc2Vec model with randomly sampled 32000 private companies and
    all of the public companies available in that year.  
    
     * To sample 32000 private companies, please execute following script:  
        ```commandline
        python create_training_sample.py <year>
        ```  
        For example to create sample for year 2000, execute following command:
        ```commandline
        python create_training_sample.py 2000
        ```
        Note: year should be greater than 1995.  
        For our example, this program will create a folder ```2000```. And then will ouput a file with 32000 companies url named
        as ```pre_training_tags_2000.txt```
        
     * With these 32000 public companies and all the available public companies in that year are taken as input for our 
     doc2vec training script. And we can run the training as follows.  
         ```commandline
         python doc2vec.py <year>  
         ```
       For example to create sample for year 2000, execute following command:
       ```commandline
       python doc2vec.py 2000  
       ```  
       
       For our example, after successful execution of this program, it'll store the model in ```2000/model/``` directory with
       filename ```doc2vec_model_2000```, and also will output a file with all the companies for which vectors can be found
       in that model, named as ```post_training_tags_2000.txt```.
       
       Currently, we have following training parameters:
       ```commandline
       vector_size=200, min_count=5, epochs=100, workers=16, hs=1, window=8
       ``` 
       As we already have calculated vectors for all the public companies, we generate the peers file for public companies
       in this run only. The peers file will have a header as following:
       ```commandline
       focal_firmid    rival_firmid    wtnic_score
       ```
       The peer file can be found at `2000/public_peer_dir/public_peers_file_2000.csv`. This peer file will contain all the
       pair wise similarity scores for public companies.


 * ###Evaluating public companies' competition
    In this section we will calculate the R-square value using the peer file generated in the above step. 
    
     * Following script execution will take 2% top peers according to the similarity scores and join them with available 
     profit and sales data for each company and then create an intermediate output file with all the companies and their profit 
     and sales, including monopolies.  
     
         ```commandline
         python generate_LR_input.py <year>    
         ```
         
        For our example, it'll create an output file at `2000/public_LR_dir/public_peers_file_2000_LR.csv`.  
        
    * In the following script, we'll take this above file as input to our Linear Regression model to calculate R-square
    value.     
        
        ```commandline
        python LR_model.py <year>
        ```
        
        For our example, it'll create an output file at `2000/public_LR_dir/Rsquare_output_2000.json`, which will have the
evaluated R-square value. 


   
 * ###Evaluating private companies' competition
    In this section, we will try to evaluate the accuracy of our model by testing the entire network of private companies.
As the training model only contains 32000 randomly sampled private companies and their vectors, in this following step
we will infer vectors for all the remaining companies by using the pre-trained model for that year. And finally, we'll 
take top 2% of the peers according to the similarity scores, and then check 2, 3, 4, 5, 6 digits NAICS code overlap 
between them. For this exercise, we have to run following scripts in order. 

    * Following script takes the pre-trained model, and a list of private companies for which we have data available in
    that year, and infers vector for all of those companies. Execute the script as follows:
    
        ```commandline
        python infer_unseen_docs.py <year> <threshold> <top>
        ```
        
        After succesfull execution of this program, it'll store all the calculated vectors for companies in the file at
        `2000/private_keyedvectors_2000`.
        
        For this program, we have two additional parameters which are used to prune the unnecessary peer score calculation.
        `threshold` is a floating point number, which is used as a threshold for similarity score, below which we won't 
        store the peers in the output file. As the number of available companies in each year is really high, we don't want
        to calculate peer similarity score for all of them. So `top` can be a floating point number which tells the program
        to consider only top% peers per each company.
        
        For example:
        ```commandline
        python infer_unseen_docs.py 2000 0.24 15
        ```
        According to our example, this program will store the peer file at 
        `2000/private_peer_dir/private_peers_file_2000.csv`, which will contain
        pair wise similarities between all the companies which has similarity score greater than 0.24. And it only 
        calculates pair wise similarities for top 15% peers for each company. This peer file has following header:
        
        ```commandline
       focal_firmid    rival_firmid    wtnic_score
       ```
    
    * To get the top 2% of the most similar peers, we need to sort this file using `wtnic_score`. Please execute following 
    `Unix` command to sort it efficiently.
    
        ```commandline
        sort -k3 -g -r -T <temp_dir_loc> -o <output_filename> <input_filename>
        ```
        
        For our example,
        ```commandline
        sort -k3 -g -r -T 2000/private_peer_dir/ -o 2000/private_peer_dir/private_peers_file_2000.csv 2000/private_peer_dir/private_peers_file_2000.csv
        ```
        If we specify input and output file as the same, it'll overwrite the results which are sorted according to the 
        similarity scores.
    
    * Now we want to evaluate NAICS code overlap for the top 2% peers. Please execute following script which will take 
    above sorted peer file as input and evaluate the overlap scores for 2, 3, 4, 5, 6 digits.  
    Note: There're companies which don't have NAICS code.  
    
        ```commandline
        python naics_sim_report.py <year>        
        ```
        
        According to our example, all the results can be found in the output file at `2000/naics_report_2000.txt`.
    
* Note: All the filenames and the directory names can be configurable by updating the `config.properties` file in the
`doc2vec_scripts` folder. 
    