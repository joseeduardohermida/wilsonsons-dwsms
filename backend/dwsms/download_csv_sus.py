#coding: utf-8
import sys
import os
import logging as log
import wget
from glob import glob
import pandas as pd
import sqlite3  
import pymongo
import json
import csv
import dateutil.parser
from datetime import datetime
import time

sys.path.insert(0, 'C:\\Desenv\\GitRep\\wilsonsons-wspython-util\\')
from wspython import wspython_mongodb_dao as mongodao

APP_ROOT                 = 'C:/Desenv/GitRep/wilsonsons-dwsms/backend/dwsms'
CSV_ORIGINAL_FOLDER_TEMP = APP_ROOT + '/temp_csv'

DATABASE_FOLDER  = 'C:/Desenv/GitRep/wilsonsons-dwsms/backend/dwsms/db/dw-sms.db'
LOG_FILE_NAME    = 'download-csv-sus.log'

COLLECTION_DATASUS_COVID19      = 'datasus_covid19'
COLLECTION_DATASUS_COVID19_TMP  = 'datasus_covid19_tmp'

MONGO_DB_NAME                   = 'dwsms'

# configuração do arquivo de log
log.basicConfig( level=log.INFO
               , format='%(asctime)s' + ' [dw-sms]' + '[%(levelname)s] %(message)s'
               , datefmt='%d/%m/%Y %H:%M:%S'
               , handlers=[
                         log.FileHandler(LOG_FILE_NAME, mode='w'),
                         log.StreamHandler()
                         ]                
                ) 

def downloadFileFromUrl( file_url : str):
    """ Faz o download de um arquivo à partir da url """
    
    delete_folder(CSV_ORIGINAL_FOLDER_TEMP)
    
    try:  
        
        log.info('iniciando download do arquivo: ' + str(file_url))

        urlSplit = file_url.split('/')
        fileName = urlSplit[len(urlSplit)-1]
        
        dest_file = CSV_ORIGINAL_FOLDER_TEMP + '\\' + fileName
        
        wget.download(file_url, dest_file)
       
        fileSize = os.path.getsize(dest_file)

        return fileSize        

    except Exception as erro:
        msg_erro = str(erro)
        log.error(msg_erro)
        raise Exception(msg_erro)         
    
def limpa_arquivos():    

    stocked_files = sorted(glob('temp_csv/*.csv', ))
    cdir         = os.path.dirname(__file__)
    
    for arq in stocked_files:
        
        file_res  = os.path.join(cdir, arq)    
    
        fileToClean = open(file_res, 'r')
        reader_list = list(csv.reader(fileToClean))
        
        arq_out = APP_ROOT + '/' + str(arq) + '.clean.csv'
        
        clean_file = open(arq_out, 'w')
        
        for line_list in reader_list:
            
            line = line_list[0]
            
            line_clean = line.replace('\'', '')            
            clean_file.write(line_clean)
            clean_file.write("\n")
            
        clean_file.close()     
    
def load_csv_mongodb():    
    
    stocked_files = sorted(glob('temp_csv/*.csv.clean.csv', ))

    cdir            = os.path.dirname(__file__)
    
    for arq in stocked_files:
        
        file_res        = os.path.join(cdir, arq)
        
        print('lendo o csv')
        df = pd.read_csv(file_res, encoding='latin1', sep=';')
        
        #-- undefined
        print('fazendo replace dos valores...')

        #df.replace(['  ']       , ''  , inplace=True)
        #df.replace(['undefined'], None, inplace=True)
        
        df.replace(['undefined'],pd.NA, inplace=True)
        #df.replace([null],None       , inplace=True)        

        #df["ÿid"].               replace({df["ÿid"]               : df["ÿid"]}               , inplace=True)
        #df["dataNotificacao"].   replace({df["dataNotificacao"]   : df["dataNotificacao"]}   , inplace=True)
        
        #df['dataNotificacao']   = df['dataNotificacao'].replace(['old value'],'new value')
        
        #df["dataInicioSintomas"].replace({df["dataInicioSintomas"]: df["dataInicioSintomas"]}, inplace=True)
        #df["dataNascimento"].    replace({df["dataNascimento"]    : df["dataNascimento"]}    , inplace=True)
        #df["sintomas"].          replace({df["sintomas"]          : df["sintomas"]}          , inplace=True)
        ##df["profissionalSaude"]. replace({df["profissionalSaude"] : df["profissionalSaude"]} , inplace=True)
        #df["cbo"].               replace({df["cbo"]               : df["cbo"]}               , inplace=True)   
        
        # df["condicoes"].replace({df["dataNotificacao"]: check_strip(df["dataNotificacao"]}, inplace=True)
        # df["estadoTeste"].replace({df["dataNotificacao"]: check_strip(df["dataNotificacao"]}, inplace=True)   
        # df["dataTeste"].replace({df["dataNotificacao"]: check_strip(df["dataNotificacao"]}, inplace=True)
        # df["tipoTeste"].replace({df["dataNotificacao"]: check_strip(df["dataNotificacao"]}, inplace=True)
        # df["resultadoTeste"].replace({df["dataNotificacao"]: check_strip(df["dataNotificacao"]}, inplace=True)
        # df["paisOrigem"].replace({df["dataNotificacao"]: check_strip(df["dataNotificacao"]}, inplace=True)  
        # df["sexo"].replace({df["dataNotificacao"]: check_strip(df["dataNotificacao"]}, inplace=True)    
        # df["estado"].replace({df["dataNotificacao"]: check_strip(df["dataNotificacao"]}, inplace=True)
        # df["estadoIBGE"].replace({df["dataNotificacao"]: check_strip(df["dataNotificacao"]}, inplace=True)
        # df["municipio"].replace({df["dataNotificacao"]: check_strip(df["dataNotificacao"]}, inplace=True)
        # df["municipioIBGE"].replace({df["dataNotificacao"]: check_strip(df["dataNotificacao"]}, inplace=True)
        # df["origem"].replace({df["dataNotificacao"]: check_strip(df["dataNotificacao"]}, inplace=True)
        # df["cnes"].replace({df["dataNotificacao"]: check_strip(df["dataNotificacao"]}, inplace=True)
        # df["estadoNotificacao"].replace({df["dataNotificacao"]: check_strip(df["dataNotificacao"]}, inplace=True)
        # df["estadoNotificacaoIBGE"].replace({df["dataNotificacao"]: check_strip(df["dataNotificacao"]}, inplace=True)
        # df["municipioNotificacao"].replace({df["dataNotificacao"]: check_strip(df["dataNotificacao"]}, inplace=True)
        # df["municipioNotificacaoIBGE"].replace({df["dataNotificacao"]: check_strip(df["dataNotificacao"]}, inplace=True)
        
        print('convertendo df para json')
        data_json = json.loads(df.to_json(orient='records'))
        
        print('gravando no mongo')
        dao = mongodao.MongoDBDAO(MONGO_DB_NAME)
        mongo_conn = dao.get_connection()
        mongo_conn[COLLECTION_DATASUS_COVID19_TMP].insert_many(data_json)        
        
def copy_temp_final ():
    
    dao = mongodao.MongoDBDAO(MONGO_DB_NAME)
    
    mongo_conn = dao.get_connection()
    
    dao.create_collection(COLLECTION_DATASUS_COVID19)
    
    db_collection     = mongo_conn[COLLECTION_DATASUS_COVID19]
    db_collection_tmp = mongo_conn[COLLECTION_DATASUS_COVID19_TMP]
    
    for doc in db_collection_tmp.find():        
        
        row = get_row_datasus_covid19_to(doc, True)        
        db_collection.insert_one(row)                  
        
# def update_columns_mongo():    
    
#     mng_client      = pymongo.MongoClient('localhost', 27017)
#     mng_db          = mng_client['dwsms']
#     db_collection   = mng_db['datasus_covid19_tmp']
    
#     for doc in db_collection.find():
        
#        dataNotificacao    = doc['dataNotificacao']
#        dataNotificacaoISO = dateutil.parser.parse(dataNotificacao)
       
#        print('type',type(dataNotificacao))
#        print('type', type(dataNotificacaoISO))
        
    #    db_collection.update_one(
    #                              {}
    #                            , {
    #                               '$set' : { 'dataNotificacaoISO' : dataNotificacaoISO }
    #                              } 
    #                            ) 
        #print(doc);
    
    
    #db_cn           = mng_db['datasus_covid19_tmp']   
    
    #mycol           = mng_db['dataNotificacao']
    
    #for x in mycol.find({},{ "_id": 0, "name": 1, "address": 1 }):
    #print(x)
    
    #db_cn.update_one(myquery, newvalues)
   
def merge_csv():

    stocked_files = sorted(glob('temp_csv/*.csv', ))
    print(stocked_files)
    
    df_concat = pd.concat((pd.read_csv(file, encoding ='latin1', sep=';', header=None).assign(filename=file)
                                    for file in stocked_files), ignore_index=True)
    
    cnx_sqllite = sqlite3.connect(DATABASE_FOLDER)
    
    df_concat.to_sql('sus-rj', cnx_sqllite, if_exists='append', index=False)
    
    cnx_sqllite.close()

def delete_folder( folder_path : str):
    """ Apaga os arquivos de uma determinada pasta """

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            os.remove(os.path.join(root, file))
            
def get_now():
    return datetime.strptime(time.strftime("%d/%m/%Y %H:%M:%S"), "%d/%m/%Y %H:%M:%S")

def to_date( string_date : str ):
    return datetime.strptime(string_date, "%d/%m/%Y %H:%M:%S")   

def check_strip ( valor ) :
    
    if type(valor) == float:
        return valor
    else:
    
        if valor != None:
            return valor.strip()   
        else:
            return valor

def get_row_datasus_covid19_to ( row, temp : bool ):    
    
    if temp:
        campo_yud = 'ÿid'
    else:
        campo_yud = 'yid'
    
    campo_yud                = check_strip(row[campo_yud])   
    dataNotificacao          = check_strip(row['dataNotificacao'])
    dataInicioSintomas       = check_strip(row['dataInicioSintomas'])
    dataNascimento           = check_strip(row['dataNascimento'])
    sintomas                 = check_strip(row['sintomas'])
    profissionalSaude        = check_strip(row['profissionalSaude'])
    cbo                      = check_strip(row['cbo'])    
    condicoes                = check_strip(row['condicoes'])
    estadoTeste              = check_strip(row['estadoTeste'])   
    dataTeste                = check_strip(row['dataTeste'])
    tipoTeste                = check_strip(row['tipoTeste'])
    resultadoTeste           = check_strip(row['resultadoTeste'])
    paisOrigem               = check_strip(row['paisOrigem'])  
    sexo                     = check_strip(row['sexo'])    
    estado                   = check_strip(row['estado'])
    estadoIBGE               = check_strip(row['estadoIBGE'])
    municipio                = check_strip(row['municipio'])
    municipioIBGE            = check_strip(row['municipioIBGE'])
    origem                   = check_strip(row['origem'])
    cnes                     = check_strip(row['cnes'])
    estadoNotificacao        = check_strip(row['estadoNotificacao'])
    estadoNotificacaoIBGE    = check_strip(row['estadoNotificacaoIBGE'])
    municipioNotificacao     = check_strip(row['municipioNotificacao'])
    municipioNotificacaoIBGE = check_strip(row['municipioNotificacaoIBGE'])
    
    if paisOrigem == 'undefined':
       paisOrigem = None
        
    if origem == 'undefined':
        origem = 'None'      
   
    doc = { 'yid'                      : campo_yud               
          , 'dataNotificacao'          : dataNotificacao         
          , 'dataInicioSintomas'       : dataInicioSintomas      
          , 'dataNascimento'           : dataNascimento          
          , 'sintomas'                 : sintomas                
          , 'profissionalSaude'        : profissionalSaude       
          , 'cbo'                      : cbo                     
          , 'condicoes'                : condicoes               
          , 'estadoTeste'              : estadoTeste             
          , 'dataTeste'                : dataTeste               
          , 'tipoTeste'                : tipoTeste               
          , 'resultadoTeste'           : resultadoTeste          
          , 'paisOrigem'               : paisOrigem              
          , 'sexo'                     : sexo                    
          , 'estado'                   : estado                  
          , 'estadoIBGE'               : estadoIBGE              
          , 'municipio'                : municipio               
          , 'municipioIBGE'            : municipioIBGE           
          , 'origem'                   : origem                  
          , 'cnes'                     : cnes                    
          , 'estadoNotificacao'        : estadoNotificacao       
          , 'estadoNotificacaoIBGE'    : estadoNotificacaoIBGE   
          , 'municipioNotificacao'     : municipioNotificacao    
          , 'municipioNotificacaoIBGE' : municipioNotificacaoIBGE
          }
    
    return doc

# def get_json_validator_create_doc ():
#     json_validator = { 
#                         'validator': { 
#                                         '$jsonSchema' : {
#                                                         'bsonType' : "object",
#                                                         'required': [ "dataNotificacao" ],
#                                                         'properties': {
#                                                                         'yid': {
#                                                                             'bsonType'    : 'string',
#                                                                             'description' : 'yid'
#                                                                         },
#                                                                         'dataNotificacao': {
#                                                                             'bsonType' : 'timestamp',
#                                                                             'description': 'dataNotificacao'
#                                                                         },
#                                                                         'dataInicioSintomas': {
#                                                                             'bsonType' : 'timestamp',
#                                                                             'description': 'dataInicioSintomas'
#                                                                         },
#                                                                         'dataNascimento': {
#                                                                             'bsonType' : 'timestamp',
#                                                                             'description': 'dataNascimento'
#                                                                         },
#                                                                         'sintomas': {
#                                                                             'bsonType' : 'string',
#                                                                             'description': 'sintomas'
#                                                                         },
#                                                                         'profissionalSaude': {
#                                                                             'bsonType' : 'string',
#                                                                             'description': 'profissionalSaude'
#                                                                         },
#                                                                         'cbo': {
#                                                                             'bsonType' : 'string',
#                                                                             'description': 'cbo'
#                                                                         },
#                                                                         'estadoTeste': {
#                                                                             'bsonType' : 'string',
#                                                                             'description': 'estadoTeste'
#                                                                         },
#                                                                         'dataTeste': {
#                                                                             'bsonType' : 'string',
#                                                                             'description': 'sintomas'
#                                                                         },
#                                                                         'tipoTeste': {
#                                                                             'bsonType' : 'string',
#                                                                             'description': 'tipoTeste'
#                                                                         },
#                                                                         'paisOrigem': {
#                                                                             'bsonType' : 'string',
#                                                                             'description': 'paisOrigem'
#                                                                         },
#                                                                         'sexo': {
#                                                                             'bsonType' : 'string',
#                                                                             'description': 'sexo'
#                                                                         },
#                                                                         'estado': {
#                                                                             'bsonType' : 'string',
#                                                                             'description': 'estado'
#                                                                         },
#                                                                         'estadoIBGE': {
#                                                                             'bsonType' : 'long',
#                                                                             'description': 'estadoIBGE'
#                                                                         },
#                                                                         'municipio': {
#                                                                             'bsonType' : 'string',
#                                                                             'description': 'municipio'
#                                                                         },
#                                                                         'origem': {
#                                                                             'bsonType' : 'string',
#                                                                             'description': 'origem'
#                                                                         },
#                                                                         'cnes': {
#                                                                             'bsonType' : 'string',
#                                                                             'description': 'cnes'
#                                                                         },
#                                                                         'estadoNotificacao': {
#                                                                             'bsonType' : 'string',
#                                                                             'description': 'estadoNotificacao'
#                                                                         },
#                                                                         'estadoNotificacaoIBGE': {
#                                                                             'bsonType' : 'long',
#                                                                             'description': 'estadoNotificacaoIBGE'
#                                                                         },
#                                                                         'municipioNotificacao': {
#                                                                             'bsonType' : 'string',
#                                                                             'description': 'municipioNotificacao'
#                                                                         },
#                                                                         'municipioNotificacaoIBGE': {
#                                                                             'bsonType' : 'long',
#                                                                             'description': 'municipioNotificacaoIBGE'
#                                                                         }
#                                                                     }
#                                                         } 
#                                      }
#                       }
#     return json_validator
    
    

if __name__ == '__main__':
    
    load_csv_mongodb()
   #copy_temp_final()