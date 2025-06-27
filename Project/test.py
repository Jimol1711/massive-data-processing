from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, sum as spark_sum, avg, first, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

def find_most_frustrating_problems(top_n=20):

    # Inicializar sesión de Spark
    spark = SparkSession.builder \
        .appName("Codeforces Most Frustrating Problems") \
        .getOrCreate()

    # Definir el esquema para leer el TSV
    schema = StructType([
        StructField("submission_id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("contestId", StringType(), True),
        StructField("problem_index", StringType(), True),
        StructField("problem_id", StringType(), True),
        StructField("programmingLanguage", StringType(), True),
        StructField("verdict", StringType(), True),
        StructField("testset", StringType(), True),
        StructField("passedTestCount", IntegerType(), True),
        StructField("timeConsumedMillis", IntegerType(), True),
        StructField("memoryConsumedBytes", LongType(), True),
        StructField("creationTimeSeconds", LongType(), True),
        StructField("original_code", StringType(), True)
    ])

    # Leer los datos TSV
    df = spark.read \
        .option("sep", "\t") \
        .option("header", "true") \
        .option("nullValue", "null") \
        .schema(schema) \
        .csv("hdfs://cm:9000/uhadoop2025/projects/codeforces/data/codeforces-data-sample.tsv")

    # Filtrar solo submissions relevantes (OK o fallidas)
    relevant_verdicts = ["OK", "WRONG_ANSWER", "TIME_LIMIT_EXCEEDED",
                        "MEMORY_LIMIT_EXCEEDED", "RUNTIME_ERROR", "COMPILATION_ERROR"]
    df = df.filter(col("verdict").isin(relevant_verdicts))
    print("Después del df.filter", df.count())
    # Crear una columna que indique si el envío fue exitoso
    df = df.withColumn("is_accepted", when(col("verdict") == "OK", 1).otherwise(0))
    print("Después del df.withColumn", df.count())
    # Ordenar por usuario, problema y tiempo de creación
    window_spec = Window.partitionBy("problem_id", "submission_id").orderBy("creationTimeSeconds")

    # Para cada usuario-problema, identificar el primer Accepted y contar intentos previos
    df_with_rank = df.withColumn("rank", row_number().over(window_spec))
    print("df_with_rank", df_with_rank.count())

    # Identificar el primer Accepted para cada usuario-problema
    first_accepts = df_with_rank.filter(col("is_accepted") == 1) \
        .groupBy("problem_id", "submission_id") \
        .agg(first("rank").alias("first_accept_rank"))
    print("first_accepts", first_accepts.count())

    # Unir con los datos originales y contar intentos fallidos antes del primer Accepted
    joined = df_with_rank.join(first_accepts, ["problem_id", "submission_id"], "left")
    print("joined", joined.count())
    user_problem_stats = joined.filter(
        (col("rank") < col("first_accept_rank")) | (col("first_accept_rank").isNull())
    ).groupBy("problem_id", "submission_id") \
     .agg(
         spark_sum("is_accepted").alias("has_accepted"),
         count(when(col("is_accepted") == 0, True)).alias("failed_attempts_before_accepted"),
         first("problem_index").alias("problem_index"),
         first("contestId").alias("contestId")
     )
    print("user_problem_stats", user_problem_stats.count())
    # Filtrar solo usuarios que resolvieron el problema (al menos un Accepted)
    solved_problems = user_problem_stats.filter(col("has_accepted") > 0)
    print("solved_problems", solved_problems.count())
    # Calcular estadísticas por problema
    problem_stats = solved_problems.groupBy("problem_id", "contestId", "problem_index") \
        .agg(
            avg("failed_attempts_before_accepted").alias("avg_failed_attempts"),
            count("submission_id").alias("solved_by_count")
        ) \
        .filter(col("solved_by_count") >= 10)  # Filtrar problemas con al menos 10 soluciones
    print("problem_stats", problem_stats.count())
    # Ordenar por frustración (promedio de intentos fallidos)
    most_frustrating = problem_stats.orderBy(col("avg_failed_attempts").desc()).limit(top_n)
    print("most_frustrating", most_frustrating.count())

    # Mostrar resultados
    print(f"\nTop {top_n} problemas más frustrantes de Codeforces:")
    most_frustrating.show(truncate=False)

    # Guardar resultados (en formato parquet por eficiencia)
    most_frustrating.write.mode("overwrite").parquet("hdfs://cm:9000/uhadoop2025/projects/codeforces/results/")

    # Guardar como CSV legible
    most_frustrating.write.mode("overwrite") \
        .option("header", "true") \
        .csv("hdfs://cm:9000/uhadoop2025/projects/codeforces/results/" + "_csv")

    spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Encuentra los problemas más frustrantes de Codeforces")
    parser.add_argument("--top", type=int, default=20, help="Número de problemas a mostrar")

    args = parser.parse_args()

    find_most_frustrating_problems(args.top)

