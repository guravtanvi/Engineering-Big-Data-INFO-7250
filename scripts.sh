#!/bin/bash


# Copying the data to HDFS

hadoop fs -put ~/Downloads/US_Accidents_Dec20.csv /
hadoop fs -put ~/Downloads/WindDirection.csv /


# Simple MR Job: Number of Traffic Accidents occurred by State

hadoop jar /home/ebd/Documents/HadoopProjects/USTrafficAccidentsProject/target/USTrafficAccidentsProject-1.0-SNAPSHOT.jar Question1.DriverClass /US_Accidents_Dec20.csv /Question1_Output


# Simple MR Job: Number of Accidents near Junction VS not a Junction

hadoop jar /home/ebd/Documents/HadoopProjects/USTrafficAccidentsProject/target/USTrafficAccidentsProject-1.0-SNAPSHOT.jar Question2.DriverClass /US_Accidents_Dec20.csv /Question2_Output


# Numerical Summarization: Average, Min, Max Visibility (miles), Max StartDate Per Accident Severity

hadoop jar /home/ebd/Documents/HadoopProjects/USTrafficAccidentsProject/target/USTrafficAccidentsProject-1.0-SNAPSHOT.jar Question3.DriverClass /US_Accidents_Dec20.csv /Question3_Output



# Numerical Summarization: Most Recent (Max) Accident Date Per City


hadoop jar /home/ebd/Documents/HadoopProjects/USTrafficAccidentsProject/target/USTrafficAccidentsProject-1.0-SNAPSHOT.jar Question4.DriverClass /US_Accidents_Dec20.csv /Question4_Output


# Numerical Summarization: Percentage of Accidents in each Month

hadoop jar /home/ebd/Documents/HadoopProjects/USTrafficAccidentsProject/target/USTrafficAccidentsProject-1.0-SNAPSHOT.jar Question5.DriverClass /US_Accidents_Dec20.csv /Question5_Output


# Top ‘N’ Filtering Pattern: Top 10 States with Highest Number of Accidents

hadoop jar /home/ebd/Documents/HadoopProjects/USTrafficAccidentsProject/target/USTrafficAccidentsProject-1.0-SNAPSHOT.jar Question6.DriverClass /Question1_Output/part-r-00000 /Question6_Output


# Top ‘N’ Filtering Pattern: Top 5 Accident Conducive Weather Conditions

hadoop jar /home/ebd/Documents/HadoopProjects/USTrafficAccidentsProject/target/USTrafficAccidentsProject-1.0-SNAPSHOT.jar Question7.a.DriverClass /US_Accidents_Dec20.csv /Question7a_Output


hadoop jar /home/ebd/Documents/HadoopProjects/USTrafficAccidentsProject/target/USTrafficAccidentsProject-1.0-SNAPSHOT.jar Question7.b.DriverClass /Question7a_Output/part-r-00000 /Question7b_Output


# Secondary Sorting: Number of Accidents Per County Per Year

hadoop jar /home/ebd/Documents/HadoopProjects/USTrafficAccidentsProject/target/USTrafficAccidentsProject-1.0-SNAPSHOT.jar Question8.DriverClass /US_Accidents_Dec20.csv /Question8_Output


# Partitioning: Partitioning based on Wind Directions

hadoop jar /home/ebd/Documents/HadoopProjects/USTrafficAccidentsProject/target/USTrafficAccidentsProject-1.0-SNAPSHOT.jar Question9.DriverClass /US_Accidents_Dec20.csv /WindDirection.csv /Question9_Output


# Inverted Index Pattern: Cities present in each County


hadoop jar /home/ebd/Documents/HadoopProjects/USTrafficAccidentsProject/target/USTrafficAccidentsProject-1.0-SNAPSHOT.jar Question10.DriverClass /US_Accidents_Dec20.csv /Question10_Output


# Binning: Binning based on Accidents on each side of the road (Left/Right)

hadoop jar /home/ebd/Documents/HadoopProjects/USTrafficAccidentsProject/target/USTrafficAccidentsProject-1.0-SNAPSHOT.jar Question11.Driver /US_Accidents_Dec20.csv /Question11_Output