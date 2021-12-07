import sys
import os
import traceback
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession
from optparse import OptionParser
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

def executeFPTree(sc, in_file, min_confidence, min_support):
    try:
        items = sc.textFile(in_file).map(lambda x : [ item.strip() for item in x.split(',')])
        trans = items.count()
        itemsset = items.flatMap(lambda x: x)
        itemcount = itemsset.distinct().count()
        table=items.zipWithIndex().map(lambda x : [x[1], x[0]])
                                   
        df = table.toDF(["id", "items"])
   
        fpGrowth = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_confidence)
        model = fpGrowth.fit(df)

        print('\n--------------------- CONFIGURATION DETAIL ---------------------\n\n')
        
        print('\tDataset=%s'%(in_file))
        print('\tsupport=%s'%(min_support))
        print('\tconfidence=%s'%(min_confidence))
        print('\titemcount=%s'%(itemcount))
        print('\tnumtrans=%s'%(trans))

        print('\n----------------------------- FREQUENCY OF ITEM -------------\n\n')
        
        # Frequent itemsets.
        df = model.freqItemsets
        supUdf = udf(lambda x:  '%s'%(x/trans), StringType()) 
        df = df.withColumn('support', supUdf("freq"))
        df.show(model.freqItemsets.count(), False)
        
        df = model.associationRules.drop('lift')
        print('\n----------------------------- ASSOCIATION RULES AND CONFIDENCE -------------\n\n')
        # Association rules.
        df.show(model.associationRules.count(), False)

    except:
        print('Error in FP Tree')
        traceback.print_exc()
        
if __name__ == '__main__':
    masterNode = 'spark://192.168.56.50:7077'
    fileName = os.path.abspath('./data/items.csv')
    min_support=0.5
    min_confidence=0.8

    conf = SparkConf().setAppName("FPTee").setMaster(masterNode)
    ss = SparkSession.builder.config(conf=conf).getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")
    sc = ss.sparkContext
    executeFPTree(sc, fileName, min_confidence, min_support)
    ss.stop()
