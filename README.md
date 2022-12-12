Project: Data Lake

     A music streaming startup, Sparkify, has grown their user base and song database even more and want to move 
     their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on 
     the app, as well as a directory with JSON metadata on the songs in their app.
     
     This application will build a ETL pipelne which loads data from json files on AWS s3, processes 
     into appropriate fact and dimension tables and stores it back in s3
    

Project Repository files:

    There are 2 files involved (dl.cfg,etl.py).

    dl.cfg  :- Config information file which is read by etl.py
    etl.py  :- is the main data processing engine , reads json files from s3 processes the data and writes it back to
               s3 in separate foldrs correspinding to fact and dimention tables on s3.
                 
                 
ETL Process(etl.py): 

      1. Spark reads data from s3 or local directory from following two sources  
           * song data from s3://udacity-dend/song_data or data/input_data/song_data  
           * log_data from s3://udacity-dend/log_data   or data/input_data/log_data  
      2. Process data with Spark  
           * transform the data and put it in the following corresponding spark dataframes  
             
             dimention table folders: users (stores user information), songs (stores song information), 
                                   artists(stores artist information), time (stores time  details)  
             Fact table folder :songplays 
                 
       3.  save the dataframes to correspoinding folders(table) as parquet files.  
              

How to run this project:  
on terminal : 
        1) python etl.py          #spark reads json files processes it and saves it into facts and dimension folders. 
