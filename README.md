# Climate Change Analysis with Spark: Leveraging Google Cloud for Scalable Data Processing

## Introduction
In this project, I analyzed temperature and CO2 emissions data to understand the impact of climate change on different countries Google Colab with Spark instance. The data is sourced from two datasets: one containing temperature data by countries from 1750 to 2015 (577k+ records), and the other containing CO2 emissions (59yrs * 264 countries) per capita across countries from 1960 to 2014. The analysis aims to uncover trends in temperature changes, identify countries with the highest temperature variations, and explore the correlation between CO2 emissions and temperature change. I later ran all the code as a spark job on Google Cloud which helped me dealing with big datasets. 

## Spark Instance Setup: 
The Spark instance for the project in Google Colab was set up by installing the Java Development Kit (JDK) version 8 headless package for Java runtime environment support. Then, Spark version 3.4.1 binary distribution with Hadoop version 3 was downloaded and extracted. The findspark library was installed to locate the Spark installation path. PySpark version 3.4.1 was installed for Python bindings. Environment variables for JAVA_HOME and SPARK_HOME were set. The findspark library was initialized, and a SparkSession was created for interaction with Spark APIs. Data files were uploaded for analysis using Google Colab's file upload feature. This setup enables distributed data processing and analysis in Google Colab with Spark.

## Analysis 
### Temperature Data
- Identified the country and year with the highest average temperature.
- Analyzed temperature changes by country and identified the top 10 countries with the biggest temperature variations.

### CO2 Emissions Data
- Merged the temperature and CO2 emissions datasets by country for the years 1960 to 2014.
- Explored the correlation between CO2 emissions and temperature change using statistical analysis.

## Conclusion
This project provides insights into the impact of climate change on global temperatures and CO2 emissions. By analyzing historical data, we gain a better understanding of the trends and patterns that contribute to climate variability. The findings can inform future research and policymaking efforts aimed at mitigating the effects of climate change.

### Google Cloud Platform (GCP) Setup
- Enabled Billing using free credits on GCP platform.
- Enabled necessary APIs such as Dataproc API and Cloud Storage API.
- Stored all files, code, and datasets in a GCP bucket for data storage.
- Set up a Dataproc instance in the us-west region with the lowest processor configuration to minimize costs.

### Execution
- Converted the code from Google Colab (Climate Change.ipynb) into a .py file (Submit Spark Job.py).
- Submitted a Spark job using the following command:gcloud dataproc jobs submit pyspark




