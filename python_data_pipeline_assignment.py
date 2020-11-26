#!/usr/bin/env python
# coding: utf-8

# # 01 data ingestion



import os
import pandas as pd
import numpy as np
import zipfile
import boto3
import psycopg2
import csv
from io import StringIO
from sqlalchemy import create_engine


#
# Here is some code to help you get started. This will connect to 
# s3 and retrieve earlier loans from the file area, they are 
# zipped into jsons.zip. The code will also extract the json 
# files to the /jsons folder. There is one json file per 
# load application, in total 10000 st. 
# 
# 

folder = "jsons"

if folder not in os.listdir():
    os.mkdir(folder)
    
import requests 

def download_url(url, save_path, chunk_size=128):
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

            
url = "https://training-demo-data.s3.eu-north-1.amazonaws.com/jsons.zip"
file = "jsons.zip"
download_url(url, file)

with zipfile.ZipFile(file, 'r') as zip_ref:
    zip_ref.extractall()
    print(f"extracted {file}")

# now the task is to parse the json files and collect 
# them into a pandas df. 

# useful functions here is 
# os.listdir("mapp") # list all files in the folder 
#     called "mapp" located in the current working directory
#
# pd.read_json(file) parse a json file
# 
# pd.DataFrame(list_of_series) # try to create a 
#     dataframe from a list of pandas series
# 
                
"<code here>"


# # 02 data cleaning


# in this section the task is to handle null values. 
# The strategy used here is to replace the null values 
# in the numerical column with the median, and replace the 
# categorcial (non-numerical columns) with "Unknown"

# Commands that can come in handy here is 
# 
# df.isna().sum(axis=0) # show the number of nan's per column 
#     (axis=1 for row-wise)
# 
# df["col"] or df.col # retrieve the values in the "column" 
#     column of the dataframe df
#
# df.col.mean() # retrive the mean value of the column 
#     "col" in the dataframe "df"
#
# df.col.fillna(val) #replace nan values with value: val
#
#


numeric_cols=["annual_inc", "loan_amnt", "nr_payment_remarks", "nr_loans"]


"<code here>"


# # 03 create new features and a label


## The task here is to create one new feature, namely 
# the loan_income_ratio, which is defined as
#
#                           loan amount
#  loan_income_ratio = -----------------------
#                          annual income 
#
#
# also, the label needs creation. This column should be = 1
# if the "loan_status" column is one of these values:
#
#     "Default", "In Grace Period", "Late (31-120 days)"
# 
# and = 0 otherwise
#
# Handy commands could be
# 
# df.groupby(["loan_status"])["id"].count() # count the number of rows for different loan statuses
# 
# df.loc[df["column"] == "value",] #filter a dataframe to only retrieve 
#     rows where the column "column" equals "value" 
#
# df["column"] = 0 assign value 0 to all rows in column "column"



# df["loan_income_ratio"] = ?

"<code here>"

# df["target"] = ?

bad_loan = ["Default", 
            "In Grace Period", 
            "Late (31-120 days)"] # label value 1

good_loan = ["Fully Paid"] # label value 0

"<code here>"

# # 04 feature engineering
# 

# ## 4.1 Scale numerical features

# 
# The task here is to scale all numerical features. You are free to 
# choose how you scale, some suggestions are minmax, standard 
# scaling (mean=0 and variance = 1) or similar
# 
#
# either you can do this by direct calculation (i.e df[col]-df[col].mean() etc )
# or by using a popular package called scikit-learn. Here, you may want to look 
# into the sklearn.preprocessing library 
# https://scikit-learn.org/stable/modules/classes.html#module-sklearn.preprocessing
#


# ## 4.2 Onehot encode categorical columns





# Now, the task is to one-hot encode the categorical columns. 
# for this, pd.get_dummies may be one useful function. Or you may want to look at the 
# https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.OneHotEncoder.html
    
# 
categorical_cols = ["term", "emp_length", "home_ownership", "purpose", "addr_state"]

"<code here>"


# ## 4.3 Bonus - text features

#
#
# In this section, you are supposed to make the text infomation more explicit for the ML model.
# The end goal is to make a numerical representation of the text data, and you are free to 
# choose whatever method you feel is suitable. 
# For example, you can read up on the TFIDF approach (Term Frequency-Inverse Document Freuency)
# (e.g. https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html)
# 
# or use a USE, universal sentence encoder, such as examplified here: 
# https://tfhub.dev/google/universal-sentence-encoder/1
#
# However the method, you will need to clean the text data. This function may come in handy for this:
#
#
# def clean_text_values(df, rx='[^0-9a-zA-Z]+', rep = "_"):
#     """
#     clean text data in df, using regex specified in rx. 
#     Substitute with the value of argument "rep"
#     """
#         df = df.replace(
#             rx, rep, regex=True).astype(str)
        
#         return df
#
#  
# 

"<code here>"




# # 05 write back table to a database 


#
# Now the task is to upload the prepared data to the database. 
# for this we have a postgres db hosted on AWS RDS
# Below is the credentials you need to connect. now the task is to upload 
# the table to the "de_course" database ("public" schema). 
# You may use the pd.to_sql command with the egine directly, 
# however you may find faster options in the link below...
# https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table
#
# # NOTE: Name the table "prepared_data_{NAME}" where the name is you name.



port='5432' 
user= "postgres"
password= "bizware_training_2020"
server_url = "data-engineering.c83yueos1s3z.eu-north-1.rds.amazonaws.com"
db = "de_course"
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{server_url}:{port}/{db}')


"<code here>"
