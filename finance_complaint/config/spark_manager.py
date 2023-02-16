from pyspark.sql import SparkSession

spark_session=(SparkSession.builder.master('local[*]').appName('finance_complaint')
                .config("spark.executer.instances","1")
                .config("spark.executer.memory","6g")
                .config("spark.driver.memory","6g")
                .config("spark.executer.memoryOverhead","8g")
                .getOrCreate())