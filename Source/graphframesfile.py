from pyspark import SparkContext,SQLContext
import os
os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages  graphframes:graphframes:0.8.0-spark2.4-s_2.11 pyspark-shell")
from graphframes import *
from pyspark import *
from pyspark.sql import *

sc=SparkContext().getOrCreate()
sqlContext = SQLContext(sc)

df_wordgame = sqlContext.read.format("csv").option("header", "true").csv('wordgame.csv')

df_wordgame.registerTempTable("wordgame")

print("-----------------------------wordgame---------------------------------")

df_wordgame.show()

# removing duplicates
after_duplicate=sqlContext.sql('select author,word1 as id,word2,source,sourceID from wordgame group by  author,word1,word2,source,sourceID limit 1000')
after_duplicate.registerTempTable("newwordgame")

vertices=sqlContext.sql('select author,id,word2,source,sourceID from newwordgame group by  author,id,word2,source,sourceID')
edges=sqlContext.sql('select id as src,word2 as dst, source as source from newwordgame group by id,word2,source')

# importing graph frames
from graphframes import *
graph=GraphFrame(vertices,edges)
pageRank=graph.pageRank(resetProbability=0.15,maxIter=1)
print("---------------------------vertices----------------------")
pageRank.vertices.show()
print("---------------------------edges-------------------------")
pageRank.edges.show()
