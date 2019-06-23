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


#import decision tree and train the model
from pyspark.ml.regression import DecisionTreeRegressor
dt = DecisionTreeRegressor(featuresCol ='features', labelCol = 'winplaceperc')
dt_model = dt.fit(train_df)

dt_predictions = dt_model.transform(test_df)
dt_predictions.select("prediction","winplaceperc","features").show(5)

#Calculating test score
from pyspark.ml.evaluation import RegressionEvaluator

dt_evaluator = RegressionEvaluator(predictionCol="prediction",\
labelCol="winplaceperc",metricName="r2")

print("R Squared (R2) on test data = %g" % dt_evaluator.evaluate(dt_predictions))

#Calculating RMSE
dt_evaluator = RegressionEvaluator(
    labelCol="winplaceperc", predictionCol="prediction", metricName="rmse")
rmse = dt_evaluator.evaluate(dt_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)



