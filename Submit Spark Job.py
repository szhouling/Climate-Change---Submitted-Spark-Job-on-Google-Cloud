from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min, abs, lit, year, avg

# Initialize the Spark session
spark = SparkSession.builder\
        .appName("Temperature and CO2 Analysis")\
        .getOrCreate()

# Load the data using Spark
df = spark.read.csv('gs://colacola_bigdata_hw2/globalTemp.csv', header=True, inferSchema=True)

# For highest average temperature
max_temp_value = df.agg(max("AverageTemperature")).collect()[0][0]
max_temp = df.filter(df['AverageTemperature'] == max_temp_value)
print("Highest Average Temperature Recorded:\n", max_temp.collect())

# Convert 'dt' to a date type and extract the year
df = df.withColumn("Year", year(col("dt")))
df = df.filter(col("AverageTemperature").isNotNull())

# Group by Country and Year, then aggregate
min_temps = df.groupBy("Country").agg(min("AverageTemperature").alias("min_temp"))
max_temps = df.groupBy("Country").agg(max("AverageTemperature").alias("max_temp"))

# Join min and max temps on Country
temp_change = min_temps.join(max_temps, "Country")
temp_change = temp_change.withColumn("temp_change", abs(col("max_temp") - col("min_temp")))
top_10_countries = temp_change.orderBy(col("temp_change").desc()).limit(10)
top_10_countries.show()

# Load CO2 emissions data
df_co2 = spark.read.csv('gs://colacola_bigdata_hw2/CO2 emissions.csv', header=True, inferSchema=True)

# Melt DataFrame for CO2 emissions by years
years = [str(year) for year in range(1960, 2015)]
df_co2_long = None

for year in years:
    temp_df = df_co2.select(col("Country Name").alias("Country"), col(year).alias("CO2_emissions"), lit(year).cast('integer').alias("Year"))
    if df_co2_long is None:
        df_co2_long = temp_df
    else:
        df_co2_long = df_co2_long.union(temp_df)

df_merged = df.join(df_co2_long, ["Country", "Year"], "inner")
df_merged = df_merged.filter((col("Year") >= 1960) & (col("Year") <= 2014))
df_merged.show()

# Calculate the correlation between CO2 emissions and temperature change
temp_range = df_merged.groupBy("Country", "Year").agg((max("AverageTemperature") - min("AverageTemperature")).alias("TempRange"))
co2_avg = df_merged.groupBy("Country", "Year").agg(avg("CO2_emissions").alias("AvgCO2Emissions"))
df_final = temp_range.join(co2_avg, ["Country", "Year"])
correlation = df_final.stat.corr("TempRange", "AvgCO2Emissions")
print(f"Correlation between Annual Temperature Range and CO2 Emissions: {correlation}")
