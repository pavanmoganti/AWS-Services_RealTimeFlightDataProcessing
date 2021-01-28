# AWS-Services_RealTimeFlightDataProcessing
Aviation Stack Flight data processing with AWS services

# Problem Statement: 
user and decision maker gets the updated data after 24 hours and by that time it’s too late make decision for important updated data which might be risky for airlines /aircraft / engine or business financial health.

# Solution: 
This architecture is designed/developed to get the updated data within a given interval like 30 minute or an hour for each flight flying in the sky or grounded at the airport.

# Step by Step Process Flow:
1)- API- lambda runs for real time flight, cities, airplanes and aircraft types and get data from Aviation stack.

2)- data get pushed to respective api stream and each stream has only 1 shard.

3)- once data pushed to stream then it gets consumed by kinesis firehose as per stream.

4)-kinesis has 300 seconds buffer time and data gets stored in S3.

5)-once data loaded in s3 then we run glue spark every hour for flight data and once in day for Citi, aircraft and airplane data.

6)- lambda and glue runs through cloud watch rules.

7)- table produced by glue job can be used in Athena and quickshight for analytics.

# Architecture Diagram:


![AWS Cloud Data Strivers Architecture](https://user-images.githubusercontent.com/26443357/106088112-52f46580-60f3-11eb-8724-03178c1193a3.jpg)

# List of AWS services used:
1) Glue
2) Athena
3) Kinesis Firehose
4) Kinesis Data Streams
5) Lambda
6) Cloud Watch
7) S3
8) Quick Sight


# Deployment_Lambda.zip: 
this zip file contains all the lambda python scripts and required python libraries to execute the lambda function in aws console. This is uploaded in s3 under bucket kinesis-producer-lambdas below are the list of python script used in lambda.

# Name of the python scripts:
Aircraftstream.py: this is being used in aircarftstreamlambda lambda function which will push aircraft data into the stream.
Airplanestream.py: this is being used in airplanestreamlambda lambda function, which will push airplane data into the stream
Citistream.py: this is being used in citistreamlambda lamda function, which will push city data into the stream
Flightstream.py: this is being used in flightstreamlambda lambda function, which will push flight data into the stream

# List of data stream::

![image](https://user-images.githubusercontent.com/26443357/104780268-82e45600-574e-11eb-94d0-0b2c9d996fd9.png)

# List of delivery stream:
![image](https://user-images.githubusercontent.com/26443357/104780301-8e378180-574e-11eb-9e89-aaf6360d96a3.png)

# S3 bucket which stores the raw data loaded by the stream:
Raw-aviation data-cds bucket contain the respective folder for each api data as seen the the screenshot. Later it is being used by glue to transform.

![image](https://user-images.githubusercontent.com/26443357/104780635-11f16e00-574f-11eb-84a4-9a4a248853f1.png)

# Glue Script name: 
aviation_data_load.py runs to process raw s3 data and stores transform data in s3 under bucket published-data-aviation-cds .

# S3 output bucket:

![image](https://user-images.githubusercontent.com/26443357/104780739-3f3e1c00-574f-11eb-9ad0-23fa3f7dc023.png)

# aviation_data_load.py example:

![image](https://user-images.githubusercontent.com/26443357/104780972-a2c84980-574f-11eb-9f93-ea6710e47485.png)



# Table created by glue job:

Table created by glue job can be used in Athena by selection the aviation dB database.

![image](https://user-images.githubusercontent.com/26443357/104781026-b8d60a00-574f-11eb-816f-a00b53e04881.png)

## Analytics: 
some analytics run top of the data we processed 

![image](https://user-images.githubusercontent.com/26443357/104781185-06527700-5750-11eb-86f1-2ae66a95c36f.png)





