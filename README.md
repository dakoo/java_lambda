# requirements

I want to implement a java code for AWS Lambda. I have an experience with javascript (nodejs) but I've never packaged and deployed a java-lambda project. So please help me. The input comes a self managed kafka topic and one event input contains multiple messages. We don't want to join/merge them. 

The purpose of the lambda is writing to the received messages to DynamoDB. 

The enviroment variables are as below:
1. dynamodb table name
2. parser and model class name because we want to reuse the code for multiple different topics and messages
3. dryrun: if it's set true The number of max batches, we wont' write to dynamodb table. 
4. number of batch writes. 

The steps 
1. read environment variables and validate them
2. printout the input event
3. Flatten the kafka event records into a single array. 
4. decode the records using the model class. 
5. Construct the upsert requests to dynamoDB. At this mement, we should use the version field of each record when we construct the message to write to DynamoDB. 
   Each column should have its version column. When we write to a table, the version column's value should be compared with the version in the kafka record. So if any of column versions is lower than the incoming record version, the create/update should fail. That means, we should construct updates with a condition expression to only update if all column versions are higher than the incoming message version. 
6. The upsert requests should be collected and then send a batch updates. The number of max batches should be configurabble. 

can you let me know how to set up a java project for lambda and give the implementation. I hope you to give me severl separate java files such as main, some utils for dynamodb message construct, batch writes, kafka decoding, environment variable parsing.. and so one. The more modulo design you provide, happier I feel

- I want to gradle. 
- In the model class, I don't want to add each column's version. Instead, I want to dynamically add <column_name>_version columns except the partition key and version column. 
- Also the kafka message is base64 encoded so before processsing the records, we need to decode them first.  
- I want to use lombok for model class definition

# Build

# if you have a gradle wrapper in your project:
./gradlew clean shadowJar
# or if you have Gradle installed system-wide:
gradle clean shadowJar


# Test
./gradlew test
