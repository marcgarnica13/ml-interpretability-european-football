from pyspark import SparkConf

SPARK_CONF = SparkConf()
SPARK_CONF.set('spark.logConf', 'true')
SPARK_CONF.set("spark.scheduler.mode", "FAIR")
SPARK_CONF.set(
    'spark.jars.packages',
    'com.databricks:spark-xml_2.11:0.5.0')
SPARK_CONF.set(
    'spark.ui.xXssProtection',
    '0'
)
SPARK_CONF.set('spark.executor.memory', '4G')
SPARK_CONF.set('spark.driver.memory', '14G')
SPARK_CONF.set('spark.driver.maxResultSize', '14G')
