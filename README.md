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
- I want to use lombok for model class definitions and some basic config classes
- Use log4j or slf4j
- I want to add a log messages at as many places as possible.
- Use asynchronous programming. 

Give me the full implementation

# Sample input

```
{ 
    "eventSource": "SelfManagedKafka",
    "bootstrapServers": "kafka-o2o-eats.abc.net:9092",
    "records": {
        "o2o.store.1-6": [
            {
                "topic": "o2o.store.1",
                "partition": 8,
                "offset": 150326091,
                "timestamp": 1726727253239k
                "timestampType": "LOG_APPEND_TIME",
                "key": "Nzc1OTMzMDg=,
                "value": "eyJ2ZXJzaW9uIjoxNzI2NzI3MjUzMjM4MDAwMDAwLCJpZCI6Nzc1OTMzMDgsInN0b3JlSWQiOjY3OTg1OCwibmFtZXMiOnsia29fS1IiOiLrlLjquLDsmrDsnKAg65qx7Lm066GxIiwiZW5fVVMiOiIifSwiZGVzY3JpcHRpb25zIjp7ImtvX0tSIjoi7ZKN7ISx7ZWcIOyasOycoO2BrOumvOyXkCDsg4HtgbztlZwg65S46riw7ZOo66CI6rCAIOuTpOyWtOqwgCDrlLjquLAg7Jqw7Jyg66eb7J20IOyeheyViCDqsIDrk50g7Y287KeA64qUIOuasey5tOuhsSIsImVuX1VTIjoiIn0sInRheEJhc2VUeXBlIjoiVEFYQUJMRSIsInJlc3RyaWN0aW9uVHlwZSI6Ik5PTkUiLCJkaXNwbGF5U3RhdHVzIjoiT05fU0FMRSIsInRhcmdldEF2YWlsYWJsZVRpbWUiOiIiLCJzYWxlUHJpY2UiOjM2MDAuMCwiY3VycmVuY3lUeXBlIjoiS1JXIiwiaW1hZ2VQYXRocyI6WyIvaW1hZ2UvZWF0c19jYXRhbG9nLzdjYzcvNjIzN2IwYjM0MGI1ODZmMzRmNzMyOGEyMGQxNWViY2Y3ZjhiYTA3Njc5ZDgyMjcyZjM2ZTQ5NDM0ZTVhLmpwZyJdLCJzYWxlRnJvbUF0IjoiIiwic2FsZVRvQXQiOiIiLCJkaXNoT3B0aW9ucyI6W10sIm9wZW5Ib3VycyI6W10sImRpc3Bvc2FibGUiOmZhbHNlLCJkaXNwb3NhYmxlUHJpY2UiOjAuMCwiZGVsZXRlZCI6ZmFsc2UsImRpc3BsYXlQcmljZSI6MC4wfQ==",
                "headers: []
            },            {
                "topic": "o2o.store.1",
                "partition": 8,
                "offset": 150326092,
                "timestamp": 1726727253307
                "timestampType": "LOG_APPEND_TIME",
                "key": "Nzc1OTMyOTY=,
                "value": "eyJ2ZXJzaW9uIjoxNzI2NzI3MjUzMzA2MDAwMDAwLCJpZCI6Nzc1OTMyOTYsInN0b3JlSWQiOjY3OTg1OCwibmFtZXMiOnsia29fS1IiOiLstpzstpztlaDrlYwg7Zi465GQ67O8IiwiZW5fVVMiOiIifSwiZGVzY3JpcHRpb25zIjp7ImtvX0tSIjoi7Lac7Lac7ZWg65WMIOqwhOyLneycvOuhnCDrqLnquLAg7KKL7J2AIO2PreyLoO2VmOqzoCDqs6DshoztlZwg7Zi465GQIOunmyDrp4jrk6TroIwiLCJlbl9VUyI6IiJ9LCJ0YXhCYXNlVHlwZSI6IlRBWEFCTEUiLCJyZXN0cmljdGlvblR5cGUiOiJOT05FIiwiZGlzcGxheVN0YXR1cyI6Ik9OX1NBTEUiLCJ0YXJnZXRBdmFpbGFibGVUaW1lIjoiIiwic2FsZVByaWNlIjo1MjAwLjAsImN1cnJlbmN5VHlwZSI6IktSVyIsImltYWdlUGF0aHMiOltdLCJzYWxlRnJvbUF0IjoiIiwic2FsZVRvQXQiOiIiLCJkaXNoT3B0aW9ucyI6W10sIm9wZW5Ib3VycyI6W10sImRpc3Bvc2FibGUiOmZhbHNlLCJkaXNwb3NhYmxlUHJpY2UiOjAuMCwiZGVsZXRlZCI6ZmFsc2UsImRpc3BsYXlQcmljZSI6MC4wfQ==",
                "headers: []
            }
        ]
    }
}
```
For the above implementation, 
The partition key is and decoded message looks like: 

```
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIngoreProperites(ignoreUnknown = true)
public class Dish {
   private Long id; // partition key
   private Long version; // version
   private Long storeId;
   private Map<String,String> names;
   private Map<String,String> descriptions;
   private String taxBaseType;
   private String displayStatus;
   private String targetAvailableTime;
   private Double salePrice;
   private String String currencyType;
   private List<String> imagePaths;
   private String saleFromAt;
   private String saleToAt;
   private List<DishOption> dishOptions;
   private List<DishOpenHour> openHours;
   Boolean disposable;
   Double disposablePrice;
   Boolean deleted;
   Double displayPrice;
}
```

An example decoded value is: 

```
{"version":1726727253306000000,"id":77593296,"storeId":679858,"names":{"ko_KR":"출출할때 호두볼","en_US":""},"descriptions":{"ko_KR":"출출할때 간식으로 먹기 좋은 폭신하고 고소한 호두 맛 마들렌","en_US":""},"taxBaseType":"TAXABLE","restrictionType":"NONE","displayStatus":"ON_SALE","targetAvailableTime":"","salePrice":5200.0,"currencyType":"KRW","imagePaths":[],"saleFromAt":"","saleToAt":"","dishOptions":[],"openHours":[],"disposable":false,"disposablePrice":0.0,"deleted":false,"displayPrice":0.0}
```

I already have a lot of model classes and hope to reuse them. 

# Build

# if you have a gradle wrapper in your project:
./gradlew clean shadowJar
# or if you have Gradle installed system-wide:
gradle clean shadowJar


# Test
./gradlew test
