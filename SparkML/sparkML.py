# Databricks notebook source
# DBTITLE 1,1 Load the data into Spark
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SQLContext
from pyspark.ml.linalg import VectorUDT,Vectors

def toFloat(x):
    if x == '?':
        return 5.0
    else:
        return float(x)

def doLine(l):
    item=l.split(",")
    label = 1
    if item[10]=='2':
        label=0
    return (Vectors.dense([toFloat(e) for e in item[1:10]]),label)

raw_data = sc.textFile("/FileStore/tables/breast_cancer_wisconsin-2f6e5.data")
schema = StructType([StructField("features", VectorUDT(), True),
                     StructField("label",IntegerType(),True)])
data = SQLContext(sc).createDataFrame(raw_data.map(doLine),schema)
data.show()
data.printSchema()
data.groupBy("label").count().show()

# COMMAND ----------

# DBTITLE 1,2 Splitting into training and testing
splits = data.randomSplit([9.0,1.0])
train = splits[0]
test = splits[1]
train.count()

# COMMAND ----------

# DBTITLE 1,3 Building the model
from pyspark.ml.classification import DecisionTreeClassifier
dtc = DecisionTreeClassifier()
bc_model = dtc.fit(train)

# COMMAND ----------

# DBTITLE 1,4 Testing your model
from pyspark.ml.evaluation import BinaryClassificationEvaluator
predictions = bc_model.transform(test)
evaluator = BinaryClassificationEvaluator(rawPredictionCol="prediction", labelCol="label", metricName='areaUnderROC')
areaUnderROC = evaluator.evaluate(predictions)
accuracy = predictions.filter("label=prediction").count()/test.count()
print(areaUnderROC,accuracy)

# COMMAND ----------

# DBTITLE 1,5 Improving the model
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

paramGrid = ParamGridBuilder().addGrid(dtc.maxDepth,[1,3,5]).addGrid(dtc.maxBins,[2,32]).build()
crossval = CrossValidator(estimator=dtc,estimatorParamMaps=paramGrid,evaluator=evaluator,numFolds=5)
cvModel = crossval.fit(train)
pre_cv = cvModel.transform(test)
areaUnderROC_cv = evaluator.evaluate(pre_cv)
accuracy_cv = pre_cv.filter("label=prediction").count()/test.count()
print(areaUnderROC_cv, accuracy_cv)

# COMMAND ----------

def getBestParam(cvModel):
    params = cvModel.getEstimatorParamMaps()
    avgMetrics = cvModel.avgMetrics

    all_params = list(zip(params, avgMetrics))
    best_param = sorted(all_params, key=lambda x: x[1], reverse=True)[0]
    return best_param
    
for p, v in getBestParam(cvModel)[0].items():
 	print("{}: {}".format(p.name, v))
print("areaUnderROC:", getBestParam(cvModel)[1])

# COMMAND ----------

from pyspark.ml.tuning import TrainValidationSplit
tvs = TrainValidationSplit(estimator=dtc,
                           estimatorParamMaps=paramGrid,
                           evaluator=evaluator,
                           # 80% of the data will be used for training, 20% for validation.
                           trainRatio=0.8)
tvModel = tvs.fit(train)
pre_tv = tvModel.transform(test)
areaUnderROC_tv = evaluator.evaluate(pre_tv)
accuracy_tv = pre_tv.filter("label=prediction").count()/test.count()
print(areaUnderROC_tv, accuracy_cv)


# COMMAND ----------

# DBTITLE 1,6.1 Improve the classification
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression()
lr_model = lr.fit(train)
pre_lr = lr_model.transform(test)
areaUnderROC_lr = evaluator.evaluate(pre_lr)
accuracy_lr = pre_lr.filter("label=prediction").count()/test.count()
print(areaUnderROC_lr,accuracy_lr)

# COMMAND ----------


