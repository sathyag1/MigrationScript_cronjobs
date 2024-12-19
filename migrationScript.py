from elasticsearch_dsl import connections
import json,os
import datetime
datetime.datetime.now()
now = datetime.datetime.now()
print("****",now, type(now))
from re import sub
from dotenv import load_dotenv
load_dotenv()
import spacy
nlp = spacy.load('en_core_web_sm')
from sentence_transformers import SentenceTransformer,util
model = SentenceTransformer('all-MiniLM-L6-v2')
import psycopg2
import pandas as pd
import pandas.io.sql as psql
from decimal import Decimal


Elk_Endpoint = os.getenv('ELK_ENDPOINT')
ELK_USERNAME = os.getenv('ELK_USERNAME')
ELK_PASSWORD = os.getenv('ELK_PASSWORD')
ELK_INDEX = os.getenv('ELK_INDEX')
PG_USER = os.getenv('USER')
PG_PASSWORD = os.getenv('PASSWORD')
PG_HOST = os.getenv('HOST')
PG_PORT = os.getenv('PORT')
PG_DATABASE = os.getenv('DATABASE')

es=connections.create_connection(hosts=[Elk_Endpoint],timeout=1200,http_auth=(ELK_USERNAME,ELK_PASSWORD))
es.ping()

connection = psycopg2.connect(user = PG_USER, 
                              password = PG_PASSWORD,
                              host = PG_HOST, 
                              port = PG_PORT, 
                              database = PG_DATABASE)
def Convertintonumber(x):
    try:
            if x is not None:

                x = sub(r"[-()\"#/@;:<>{}`+=~|.!?,]", "", x)
                value = int(x)
            else:
                value = 0
                
    except Exception as e: 
             value = 0
             
    return value
def ConvertintoSentence_vec(x):
        try:
            if x is not None:
                jd=clean_text(str(x))
                #print(jd)\n",
                sentence_vec=model.encode(jd),
            else :
                sentence_vec = 0
            
        except ValueError as err: 
             sentence_vec="0"
        return sentence_vec
    
def clean_text(text):
    doc = nlp(text)
    keywords = [str(token.lemma_).lower() for token in doc if token.pos_ not in ['PRON', 'PUNCT', 'SPACE'] and not token.is_stop and len(token.text)>1]
    clean_text = ' '.join(keywords)
    return clean_text

def ConverttoDatetime(x):#new function with complete datetime format
        try:
            if x is not None:
               created_dt = x.strftime('%Y-%m-%d %H:%M:%S.%f%z')
            
            else:
                created_dt = ""
        except ValueError as err: 
             created_dt = ""
    
        return created_dt
def ConvertMoneytofee(x):
        try:
            if x is not None:
                money = x
                value = Decimal(sub(r'[^\d.]', '', money))
                value = round(value)
            else:
                value = 0
                
        except ValueError as err: 
             value = 0
             
        return value
def getdataset(query_body,get_param_type):
    print(get_param_type)
    response = es.search(index=ELK_INDEX,body=query_body)
    if get_param_type == "get_latest_date" :
        data=""
        data = [x['_source'] for x in response["hits"]["hits"]]
        for i in data:
            for value in i.values():
                data = value
    else:
        data=[]
        data1 = [x['_source'] for x in response["hits"]["hits"]]
        for i in data1:
            for value in i.values():
                data.append(value) 
    return data     

query_body1 = {
     "query": {
               "match_all": {}
              },
       "_source": ["create_date"],
       "size": 1,
        "sort": [
        {
        "create_date": {
            "order": "desc"
        }
        }
       ]
    }  

create_date = getdataset(query_body1,'get_latest_date')
print("latest create date and time:", create_date)

query_body2 = {
     "query": {
               "match_all": {}
              },
       "_source": ["update_date"],
       "size": 1,
        "sort": [
        {
        "update_date": {
            "order": "desc"
        }
        }
       ]
    }  

update_date = getdataset(query_body2,'get_latest_date')
print("latest update date and time:", update_date)#,type(update_date),len(update_date))  
if len(update_date)==0:
    print("inside update_date updation")
    update_date=create_date
#sql_createquery ="select * from learning.get_course_master_vw" 
sql_createquery ="select * from learning.get_course_master_vw where course_created_date>  '%s' " % (create_date) 
print(sql_createquery)
df1= psql.read_sql(sql_createquery, connection);print("from create_date",df1.shape)

sql_updatequery="select * from learning.get_course_master_vw where course_updated_date>'%s' " % (update_date)
print(sql_updatequery)
df1= psql.read_sql(sql_createquery, connection);print("from create_date",df1.shape)
print(sql_updatequery)
df2= psql.read_sql(sql_updatequery, connection);print("from update date",df2.shape)

df = pd.concat([df1,df2])  
df = df.drop_duplicates()


Checkdfval= df.shape[0]

print(Checkdfval)

if Checkdfval == 0 :
    print("No records found")
else:
    try:
        df.rename(columns = {'course_created_date':'create_date','course_updated_date':'update_date','course_display_image_link':'course_image','course_status':'active_flag'}, inplace = True)
        
        df['create_date'] = df['create_date'].apply(lambda x: ConverttoDatetime(x))
        df['update_date'] = df['update_date'].apply(lambda x: ConverttoDatetime(x))
        df['elk_create_date'] = now.strftime('%Y-%m-%d %H:%M:%S')
        df['course_info']=df['course_field_of_study']+df['course_title']
        df['course_info_vec'] = df['course_info'].apply(lambda x: ConvertintoSentence_vec(x))
        df['Qualification_Eligibility_vec'] = df['course_qualification'].apply(lambda x: ConvertintoSentence_vec(x))
        df['course_fee'] = df['course_fee'].apply(lambda x: ConvertMoneytofee(x))
        df['course_level'] = df['course_level'].apply(lambda x: Convertintonumber(x))
        df['course_start_date'] = df['course_start_date'].apply(lambda x: ConverttoDatetime(x))
        df['course_end_date'] = df['course_end_date'].apply(lambda x: ConverttoDatetime(x))
        course_list = df['course_id'].tolist()
        print(course_list)
        
        query = {
                "query": {
                        "bool": {
                            "must" : [
                                {"terms": { "course_id.keyword": course_list}}
                                ]
                    }
            }

            }
        
        res = es.delete_by_query(index=ELK_INDEX, body=query)
        
        json_str = ""
        json_records = ""
        json_str = df.to_json(orient='records')

        json_records = json.loads(json_str)
        for json_doc in json_records:
            
            res=es.index(index=ELK_INDEX, ignore=400, document = json_doc)
            print(res)
            print("Success")
    except Exception as e:
           print(e)    
