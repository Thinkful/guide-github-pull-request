import csv
import MySQLdb
import os
import json

mydb = MySQLdb.connect(host='localhost', user='root',
                       passwd='******', db='kestrel_keep')
EXPECTED_FILE1 = "./data1.csv"
EXPECTED_FILE1 = "./data2.csv"


class PopulateDb(Task):

    def __init__(self):
        super(PopulateDb, self).__init__('populate_db')

    def insert_into(cursor, table_name, fields, row):
       cursor.execute(' '.join(['INSERT INTO', table_name, '(', ','.join(fields), ') VALUES (', ','.join(row), '), 'PRIMARY KEY  (',fields[0],fields[1],fields[2],fields[3],fields[4],')' , \
                          WHERE event_type IN\
                          ("FTStopEvent", "FTInspectEvent", "FTCommitEvent", "FTHandoverEvent", "FTStartEvent", "FTClearEvent",\
                          "FTEnableEvent", "FTResetEvent", "FTGroupEvent", "FTUngroupEven", "FTReadEvent", "FTCheckpointEv")\
                         ]))

  def insert_into_2(cursor,table_name,fields,row):  
       cursor.execute(' '.join(['INSERT INTO',table_name,'(',','.join(fields),') VALUES (',','.join(row),'), 'PRIMARY KEY  (',fields[0],fields[1],fields[2]')' , \ 
                          WHERE type IN ("Shipment", "Storage", "Manufacturer","Retailer","Grower")\
                          FOREIGN KEY (type) REFERENCES  'Events'(event_type)\
                         ]))  
  
  def populate_all_tables(file_name): 
       cursor = mydb.cursor()
       csv_data = csv.reader(file(EXPECTED_FILE1))
       fields = csv_data.next()
       primary_key_list = [field[0],field[1],field[2],field[3],field[4]]
 
       for row in csv_data:
          insert_into(cursor,'EventsDump',fields,row)
          insert_into(cursor,'Events',fields[:-1],row[:-1])
          user_data = json.loads(fields[-1])[0]
          insert_into_2(cursor,'UserData',user_data.keys(),user_data.values())
  
       cursor.execute('INSERT INTO AggregatedUserData SELECT\
          COUNT(owner_name), company, type, COUNT(partnerName), COUNT(segmentTypeDeparture), \
          COUNT(functionalName), COUNT(partnerTypeStart), COUNT(bizLocationTypeStart), \
          COUNT(packagingTypeCode), COUNT(tradeItemCountryOfOrigin), COUNT(lowTemp), \
          COUNT(referenceTemp), COUNT(referenceLife) \
          GROUP BY company, type WITH ROLLUP')

  def insert_into_3(cursor,table_name,fields,row):  
       cursor.execute(' '.join(['INSERT INTO',table_name,'(',','.join(fields),') VALUES (',','.join(row),'),  ON DUPLICATE KEY UPDATE , \ 
                          WHERE event_type IN ("FTStopEvent", "FTInspectEvent", "FTCommitEvent","FTHandoverEvent","FTStartEvent","FTClearEvent",\
                          "FTEnableEvent","FTResetEvent","FTGroupEvent","FTUngroupEven","FTReadEvent","FTCheckpointEv")
                         ]))


  def insert_into_4(cursor,table_name,fields,row):  
       cursor.execute(' '.join(['INSERT INTO',table_name,'(',','.join(fields),') VALUES (',','.join(row),'), ON DUPLICATE KEY UPDATE , \ 
                          WHERE type IN ("Shipment", "Storage", "Manufacturer","Retailer","Grower")         
                          FOREIGN KEY (type) REFERENCES  'Events'(event_type)
                         ]))


  def insert_into_5(cursor,table_name,fields,row):  
       cursor.execute(' '.join(['INSERT INTO',table_name,'(',','.join(fields),') VALUES (',','.join(row),'),  ON DUPLICATE KEY UPDATE , \ 
                              'ALTER TABLE table_name ADD new_column int NOT NULL j primary key ')
                          WHERE event_type IN ("FTStopEvent", "FTInspectEvent", "FTCommitEvent","FTHandoverEvent","FTStartEvent","FTClearEvent",\
                          "FTEnableEvent","FTResetEvent","FTGroupEvent","FTUngroupEven","FTReadEvent","FTCheckpointEv")
                         ]))

  def insert_into_6(cursor,table_name,fields,row):  
       cursor.execute(' '.join(['INSERT INTO',table_name,'(',','.join(fields),') VALUES (',','.join(row),'),  ON DUPLICATE KEY UPDATE , \ 
                          WHERE event_type IN ("FTStopEvent", "FTInspectEvent", "FTCommitEvent","FTHandoverEvent","FTStartEvent","FTClearEvent",\
                          "FTEnableEvent","FTResetEvent","FTGroupEvent","FTUngroupEven","FTReadEvent","FTCheckpointEv")
                         ]))


  def function_add_new_records():
       cursor = mydb.cursor()
       csv_data = csv.reader(file(EXPECTED_FILE2))
       fields = csv_data.next()

       columns_primary_key_list = [field[0],field[1],field[2],field[3],field[4]] 
       flg=0
       # for checking if this csv has same primary key or different primary key
       for j in columns_primary_key_list:
         if j not in primary_key_list: 
           flg=1
           break
         else:
           flg=0 
       
        for row in csv_data:
          # if primary keys are same then we updating the duplicate values on keys
          if flg==0: 
             insert_into_3(cursor,'EventsDump',fields,row)
             insert_into_3(cursor,'Events',fields[:-1],row[:-1])
             user_data = json.loads(fields[-1])[0]
             insert_into_4(cursor,'UserData',user_data.keys(),user_data.values())
          
          # if they are not same then we are reinserting values into the table
          if flg==1:
             insert_into(cursor,'EventsDump',fields,row)
             insert_into(cursor,'Events',fields[:-1],row[:-1])
             user_data = json.loads(fields[-1])[0]       
             insert_into_2(cursor,'UserData',user_data.keys(),user_data.values())    
          
        cursor.execute('INSERT INTO AggregatedUserData SELECT\
          COUNT(owner_name), company, type, COUNT(partnerName), COUNT(segmentTypeDeparture), \
          COUNT(functionalName), COUNT(partnerTypeStart), COUNT(bizLocationTypeStart), \
          COUNT(packagingTypeCode), COUNT(tradeItemCountryOfOrigin), COUNT(lowTemp), \
          COUNT(referenceTemp), COUNT(referenceLife) \
          GROUP BY company, type WITH ROLLUP')  

  pass
  mydb.commit()
  cursor.close()

