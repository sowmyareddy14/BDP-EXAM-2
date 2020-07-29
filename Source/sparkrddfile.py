import os
os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages  graphframes:graphframes:0.8.0-spark2.4-s_2.11 pyspark-shell")
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType

if _name_ == "_main_":

    spark = SparkSession.builder.appName("exam2").config("spark.some.config.option", "some-value").getOrCreate()
    sc = SparkContext.getOrCreate()

    # creating an RDD
    lines = sc.textFile("WorldCups.csv")

    # header
    header = lines.first()

    # removing header
    content = lines.filter(lambda line: line != header)

    # display rdd
    rdd = (content.map(lambda line: (line.split(","))).collect())

    print("--------------------------RDD-------------------------")
    print(rdd)

    # schema containing struct types
    schema = StructType([StructField('Year', StringType(), True),
                         StructField('Country', StringType(), True),
                         StructField('Winner', StringType(), True),
                         StructField('Runners-Up', StringType(), True),
                         StructField('Third', StringType(), True),
                         StructField('Fourth', StringType(), True),
                         StructField('GoalsScored', StringType(), True),
                         StructField('QualifiedTeams', StringType(), True),
                         StructField('MatchesPlayed', StringType(), True),
                         StructField('Attendance', StringType(), True)])


    # Create data frame from the RDD
    df = spark.createDataFrame(rdd, schema)
    df.show()

    # convert string to int
    df = df.withColumn('GoalsScored', df['GoalsScored'].cast(IntegerType()))

    # country with highest goals - RDD
    rdd1 = (content.filter(lambda line: line.split(",")[6] != "NULL")
            .map(lambda line: (line.split(",")[1], int(line.split(",")[6])))
            .takeOrdered(10, lambda x: -x[1]))

    print("--------------------Country with Highest Goals-------------")
    print(rdd1)

    # country with highest goals - DF
    df.select("Country", "GoalsScored").orderBy("GoalsScored", ascending=False).show(10, truncate=False)

    # country with highest goals - DF SQL
    df.createOrReplaceTempView("df_table")
    spark.sql(" SELECT Country,GoalsScored FROM df_table order by " +
              "GoalsScored Desc Limit 10").show()


    # Years when Host country wins - RDD
    rdd2= (content.filter(lambda line: line.split(",")[1] == line.split(",")[2])
     .map(lambda line: (line.split(",")[0], line.split(",")[1], line.split(",")[2]))
     .collect())

    print("--------------Host Country Wins------------")
    print(rdd2)

    # Host country wins - DF
    df.select("Year", "Country", "Winner").filter(df["Country"] == df["Winner"]).show()

    # Host country wins - DF SQL
    spark.sql(" SELECT Year,Country,Winner FROM df_table where Country == Winner order by Year").show()

    # Selecting winners for given years ending with 8 - RDD
    years = ["1938", "1958", "1978", "1998"]
    rdd3= (content.filter(lambda line: line.split(",")[0] in years)
     .map(lambda line: (line.split(",")[0], line.split(",")[2])).collect())
    print("--------------Winners For years ending with 8------------")
    print(rdd3)

    # Selecting winners for given years ending with 8 - DF
    df.select("Year", "Winner").filter(df.Year.isin(years)).show()

    # Selecting winners and runners for given years ending with 8 - DF SQL

    spark.sql(" SELECT Year,Winner FROM df_table  WHERE " +
              " Year IN ('1938','1958','1978','1998') ").show()


    # Displaying data for year 1958 - RDD
    rdd4=(content.filter(lambda line: line.split(",")[0] == "1958")
     .map(lambda line: (line.split(","))).collect())

    print("-------------Data For Year 1958------------")
    print(rdd4)

    # Displaying data for year 1958 - DF
    df.filter(df.Year == "1958").show()

    # Displaying data for year 1958 - DF Sql
    spark.sql(" Select * from df_table where Year == 1958 ").show()



    # Highest matches played - RDD
    rdd5=(content.filter(lambda line: line.split(",")[8] == "64")
     .map(lambda line: (line.split(","))).collect())
    print("-------------Highest Matches Played------------")
    print(rdd5)

    # Highest matches played - DF
    df = df.withColumn('MatchesPlayed', df['MatchesPlayed'].cast(IntegerType()))
    df.filter(df.MatchesPlayed == 64).show()

    # Highest matches played - DF SQL
    spark.sql(" Select * from df_table where MatchesPlayed in " +
              "(Select Max(MatchesPlayed) from df_table )").show()
