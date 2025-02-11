package com.example;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class TestUtils {

    // A corrected JSON payload as a String
    private static final String SAMPLE_KAFKA_JSON = "{\n" +
            "  \"eventSource\": \"SelfManagedKafka\",\n" +
            "  \"bootstrapServers\": \"kafka-o2o-eats.abc.net:9092\",\n" +
            "  \"records\": {\n" +
            "    \"o2o.store.1-6\": [\n" +
            "      {\n" +
            "        \"topic\": \"o2o.store.1\",\n" +
            "        \"partition\": 8,\n" +
            "        \"offset\": 150326091,\n" +
            "        \"timestamp\": 1726727253239,\n" +
            "        \"timestampType\": \"LOG_APPEND_TIME\",\n" +
            "        \"key\": \"Nzc1OTMzMDg=\",\n" +
            "        \"value\": \"eyJ2ZXJzaW9uIjoxNzI2NzI3MjUzMjM4MDAwMDAwLCJpZCI6Nzc1OTMzMDgsInN0b3JlSWQiOjY3OTg1OCwibmFtZXMiOnsia29fS1IiOiLrlLjquLDsmrDsnKAg65qx7Lm066GxIiwiZW5fVVMiOiIifSwiZGVzY3JpcHRpb25zIjp7ImtvX0tSIjoi7ZKN7ISx7ZWcIOyasOycoO2BrOumvOyXkCDsg4HtgbztlZwg65S46riw7ZOo66CI6rCAIOuTpOyWtOqwgCDrlLjquLAg7Jqw7Jyg66eb7J20IOyeheyViCDqsIDrk50g7Y287KeA64qUIOuasey5tOuhsSIsImVuX1VTIjoiIn0sInRheEJhc2VUeXBlIjoiVEFYQUJMRSIsInJlc3RyaWN0aW9uVHlwZSI6Ik5PTkUiLCJkaXNwbGF5U3RhdHVzIjoiT05fU0FMRSIsInRhcmdldEF2YWlsYWJsZVRpbWUiOiIiLCJzYWxlUHJpY2UiOjM2MDAuMCwiY3VycmVuY3lUeXBlIjoiS1JXIiwiaW1hZ2VQYXRocyI6WyIvaW1hZ2UvZWF0c19jYXRhbG9nLzdjYzcvNjIzN2IwYjM0MGI1ODZmMzRmNzMyOGEyMGQxNWViY2Y3ZjhiYTA3Njc5ZDgyMjcyZjM2ZTQ5NDM0ZTVhLmpwZyJdLCJzYWxlRnJvbUF0IjoiIiwic2FsZVRvQXQiOiIiLCJkaXNoT3B0aW9ucyI6W10sIm9wZW5Ib3VycyI6W10sImRpc3Bvc2FibGUiOmZhbHNlLCJkaXNwb3NhYmxlUHJpY2UiOjAuMCwiZGVsZXRlZCI6ZmFsc2UsImRpc3BsYXlQcmljZSI6MC4wfQ==\",\n" +
            "        \"headers\": []\n" +
            "      },\n" +
            "      {\n" +
            "        \"topic\": \"o2o.store.1\",\n" +
            "        \"partition\": 8,\n" +
            "        \"offset\": 150326092,\n" +
            "        \"timestamp\": 1726727253307,\n" +
            "        \"timestampType\": \"LOG_APPEND_TIME\",\n" +
            "        \"key\": \"Nzc1OTMyOTY=\",\n" +
            "        \"value\": \"eyJ2ZXJzaW9uIjoxNzI2NzI3MjUzMzA2MDAwMDAwLCJpZCI6Nzc1OTMyOTYsInN0b3JlSWQiOjY3OTg1OCwibmFtZXMiOnsia29fS1IiOiLstpzstpztlaDrlYwg7Zi465GQ67O8IiwiZW5fVVMiOiIifSwiZGVzY3JpcHRpb25zIjp7ImtvX0tSIjoi7Lac7Lac7ZWg65WMIOqwhOyLneycvOuhnCDrqLnquLAg7KKL7J2AIO2PreyLoO2VmOqzoCDqs6DshoztlZwg7Zi465GQIOunmyDrp4jrk6TroIwiLCJlbl9VUyI6IiJ9LCJ0YXhCYXNlVHlwZSI6IlRBWEFCTEUiLCJyZXN0cmljdGlvblR5cGUiOiJOT05FIiwiZGlzcGxheVN0YXR1cyI6Ik9OX1NBTEUiLCJ0YXJnZXRBdmFpbGFibGVUaW1lIjoiIiwic2FsZVByaWNlIjo1MjAwLjAsImN1cnJlbmN5VHlwZSI6IktSVyIsImltYWdlUGF0aHMiOltdLCJzYWxlRnJvbUF0IjoiIiwic2FsZVRvQXQiOiIiLCJkaXNoT3B0aW9ucyI6W10sIm9wZW5Ib3VycyI6W10sImRpc3Bvc2FibGUiOmZhbHNlLCJkaXNwb3NhYmxlUHJpY2UiOjAuMCwiZGVsZXRlZCI6ZmFsc2UsImRpc3BsYXlQcmljZSI6MC4wfQ==\",\n" +
            "        \"headers\": []\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "}";

    /**
     * Parses the SAMPLE_KAFKA_JSON string into a KafkaEvent
     * using Jackson.
     *
     * @return A KafkaEvent object corresponding to the sample JSON.
     * @throws IOException If parsing fails.
     */
    public static KafkaEvent buildKafkaEventFromJson() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        // The KafkaEvent class in aws-lambda-java-events is annotated for Jackson deserialization.
        // This will read "eventSource", "bootstrapServer", and "records" -> "topic/partition" map.
        return mapper.readValue(SAMPLE_KAFKA_JSON, KafkaEvent.class);
    }
}
