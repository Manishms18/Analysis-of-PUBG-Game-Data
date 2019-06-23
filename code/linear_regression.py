from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
hive_context=HiveContext(sc)

pubg=hive_context.table("pubg_new")

#Selecting columns of interest
pubg=pubg.select('boosts', 'damagedealt', 'dbnos', 'headshotkills', 'heals', 'killplace', 'killpoints', 'kills', 'killstreaks', 'longestkill', 'maxplace', 'numgroups', 'revives', 'ridedistance', 'roadkills', 'swimdistance', 'teamkills', 'vehicledestroys', 'walkdistance', 'weaponsacquired', 'winpoints', 'winplaceperc')

pubg.show(10) 
pubg.printSchema()
pubg.cache()

from pyspark.ml.feature import VectorAssembler

#Creating feature vector
vectorAssembler = VectorAssembler(inputCols = ['boosts', 'damagedealt', 'dbnos', 'headshotkills', 'heals', 'killplace', 'killpoints', 'kills', 'killstreaks', 'longestkill', 'maxplace', 'numgroups', 'revives', 'ridedistance', 'roadkills', 'swimdistance', 'teamkills', 'vehicledestroys', 'walkdistance', 'weaponsacquired', 'winpoints', 'winplaceperc'], outputCol = 'features')

#Transforming the dataframe 
pubg_df=vectorAssembler.transform(pubg)

#Selecting features and target variable from the dataframe
pubg_df = pubg_df.select(['features', 'winplaceperc'])
pubg_df.show(3)

#Splitting the data into test and train
splits = pubg_df.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]


#import linear regression and train the model
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol = 'features', labelCol='winplaceperc', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)

print("Coefficients: " + str(lr_model.coefficients))

print("Intercept: " + str(lr_model.intercept))
trainingSummary = lr_model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)


pubg_df.describe().show()

#Predicting the test set
lr_predictions = lr_model.transform(test_df)
lr_predictions.select("prediction","winplaceperc","features").show(10)

#Evaluating the model on test-set
from pyspark.ml.evaluation import RegressionEvaluator
lr_evaluator = RegressionEvaluator(predictionCol="prediction", \
                 labelCol="winplaceperc",metricName="r2")
print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))

test_result = lr_model.evaluate(test_df)
print("Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)


print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()

#Using our Linear Regression model to make some predictions:
predictions = lr_model.transform(test_df)
predictions.select("prediction","winplaceperc","features").show()



