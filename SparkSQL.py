# Suppress warnings
def warn(*args, **kwargs):
    pass

import warnings
warnings.warn = warn
warnings.filterwarnings('ignore')

# Import required libraries
import findspark
findspark.init()

from pyspark.sql import SparkSession

# Task 1 - Create a Spark session
# Initialize the Spark session with a meaningful application name
spark_session = SparkSession.builder.appName("VehicleMileageAnalysis").getOrCreate()

# Task 2 - Load the CSV file into a Spark DataFrame
# Load the CSV file into a Spark DataFrame with inferred schema and headers
mileage_data = spark_session.read.csv("mpg.csv", header=True, inferSchema=True)

# Task 3 - Create a temporary view
# Create a temporary SQL view for the DataFrame
mileage_data.createOrReplaceTempView("mileage_view")

# Task 4 - Run SQL queries on the DataFrame
# Select all cars with mileage greater than 40 MPG
high_mileage_cars = spark_session.sql("SELECT * FROM mileage_view WHERE MPG > 40")
print("Cars with mileage greater than 40 MPG:")
high_mileage_cars.show()

# Task 5 - Analyze the dataset
# 5.1 - List all unique origins of cars
unique_origins = spark_session.sql("SELECT DISTINCT Origin FROM mileage_view")
print("Unique car origins:")
unique_origins.show()

# 5.2 - Show the count of Japanese cars
japanese_car_count = spark_session.sql("SELECT COUNT(*) AS count FROM mileage_view WHERE Origin = 'Japanese'")
print("Count of Japanese cars:")
japanese_car_count.show()

# 5.3 - Count the number of cars with mileage greater than 40 MPG
high_mileage_count = spark_session.sql("SELECT COUNT(*) AS count FROM mileage_view WHERE MPG > 40")
print("Number of cars with mileage greater than 40 MPG:")
high_mileage_count.show()

# 5.4 - List the number of cars made in different years
cars_by_year = spark_session.sql("SELECT Year, COUNT(Year) AS car_count FROM mileage_view GROUP BY Year")
print("Number of cars made in each year:")
cars_by_year.show()

# 5.5 - Print the maximum MPG value
max_mpg = spark_session.sql("SELECT MAX(MPG) AS max_mpg FROM mileage_view")
print("Maximum MPG value:")
max_mpg.show()

# Stop the Spark session
spark_session.stop()
