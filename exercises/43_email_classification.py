# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Email Classification (Spark MLlib)
# MAGIC Build a simple text classification pipeline to classify emails as spam or not spam.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import functions as F

# COMMAND ----------

# Sample email data: 1 = spam, 0 = not spam
data = [
    (0, "Hi team, please review the attached document for the meeting"),
    (0, "Let's schedule a call to discuss the project timeline"),
    (0, "The quarterly report is ready for your review"),
    (0, "Can you send me the updated requirements document"),
    (1, "Congratulations you have won a free prize click here now"),
    (1, "Buy cheap products now limited time offer free shipping"),
    (1, "You are selected for a special discount claim your reward"),
    (1, "Make money fast work from home earn thousands daily free"),
    (0, "Please find the meeting notes from today's standup"),
    (0, "The deployment to production is scheduled for Friday"),
    (1, "Free gift card waiting for you click to claim now"),
    (1, "Urgent action required verify your account immediately free"),
]
df = spark.createDataFrame(data, ["label", "text"])
df.show(truncate=False)

# COMMAND ----------

# Split into train/test
train, test = df.randomSplit([0.8, 0.2], seed=42)

# COMMAND ----------

# Build ML pipeline: Tokenizer -> HashingTF -> IDF -> LogisticRegression
tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashing_tf = HashingTF(inputCol="words", outputCol="raw_features", numFeatures=256)
idf = IDF(inputCol="raw_features", outputCol="features")
lr = LogisticRegression(maxIter=10)

pipeline = Pipeline(stages=[tokenizer, hashing_tf, idf, lr])

# COMMAND ----------

# Train
model = pipeline.fit(train)

# COMMAND ----------

# Predict
predictions = model.transform(test)
predictions.select("text", "label", "prediction", "probability").show(truncate=False)

# COMMAND ----------

# Evaluate
from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator()
auc = evaluator.evaluate(predictions)
print(f"AUC: {auc:.4f}")
