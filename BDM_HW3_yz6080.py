#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import sys
import csv


if __name__=='__main__':

    if len(sys.argv) == 2:
        input_file = sys.argv[1]
        
    elif len(sys.argv) == 3:
        input_file = sys.argv[1]
        output_folder = sys.argv[2]
    
    else:
        raise ValueError("Wrong number of arguments passed in.")
    
    
    def extract(partitionId, records):
        for partitionId in range(3):
            next(records)
        import csv
        reader = csv.reader(records)
        for r in reader:
            if len(r)==18 and r[17]!='N/A':
                (product,company,date) = (r[1].lower(), r[7].upper(), r[0][0:4])
            yield ((product, company, date),1)
    sc = SparkContext()  
    
    complaint = sc.textFile(input_file)
    complaint_N =complaint.mapPartitionsWithIndex(extract)
       
    complaint_N = complaint_N.reduceByKey(lambda x,y: x+y)                     .map(lambda x: ((x[0][0],x[0][2]),(x[1],1,x[1])))                     .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], max(x[2],y[2])))                     .mapValues(lambda x: (x[0],x[1],round(x[2]*100 / x[0])))                     .sortByKey()                     .map(lambda x: (list(x[0])+[a for a in x[1]]))                     .saveAsTextFile(output_folder)

