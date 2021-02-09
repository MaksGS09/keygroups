from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def array_to_string(my_list):
    return '[' + ','.join([str(elem) for elem in my_list]) + ']'


array_to_string_udf = udf(array_to_string, StringType())


class Pipeline(object):

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def run(self):
        spark = SparkSession.builder.master("local[*]").appName("keyword groups").getOrCreate()
        input_df = spark.read.csv(self.input, header=True)
        processed_df = input_df.select(
            F.col("URL"),
            F.regexp_replace(F.col("Keyword"), r'["\[\]]', "").alias("Keywords")
        ).select(
            F.col("URL"),
            F.split(F.col("Keywords"), ",").alias("Keywords_array")
        ).select(
            F.col("URL"),
            F.explode(F.col("Keywords_array")).alias("key")
        ).cache()
        grouped_df = processed_df.groupBy("key").agg(F.collect_list(F.col("URL")
                                                                    ).alias("urls_array")
                                                     ) \
            .withColumn("array_len", F.size(F.col("urls_array"))) \
            .filter((F.col("array_len") > 3) | (F.col("array_len") == 3)) \
            .drop(F.col("array_len")) \
            .groupBy(F.col("urls_array")).agg(F.collect_list(F.col("key")
                                                             ).alias("keys_array")
                                              ).withColumn("array_len",
                                                           F.size(F.col("keys_array"))
                                                           ).filter(
            (F.col("array_len") > 2) | (F.col("array_len") == 2)) \
            .drop(F.col("array_len"))

        grouped_df.show(truncate=False)
        out_df = grouped_df.withColumn('keys', array_to_string_udf(grouped_df["keys_array"])) \
            .withColumn('urls', array_to_string_udf(grouped_df["urls_array"])) \
            .drop("keys_array", "urls_array")
        out_df.repartition(1).write.csv(self.output, header=True)


if __name__ == '__main__':
    input_path = "df.csv"
    output_path = "result_df"
    pipeline = Pipeline(input=input_path, output=output_path)
    pipeline.run()
