package com.example;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MainLambdaHandlerTest {

    @Test
    void parseSampleKafkaJson() throws Exception {
        KafkaEvent event = TestUtils.buildKafkaEventFromJson();

        assertNotNull(event);
        assertEquals("SelfManagedKafka", event.getEventSource());
//        assertEquals("kafka-o2o-eats.abc.net:9092", event.getBootstrapServer());

        // The records map should have 1 key: "o2o.store.1-6"
        assertTrue(event.getRecords().containsKey("o2o.store.1-6"));
        assertEquals(2, event.getRecords().get("o2o.store.1-6").size());

        KafkaEvent.KafkaEventRecord record1 = event.getRecords().get("o2o.store.1-6").get(0);
        assertEquals("o2o.store.1", record1.getTopic());
        assertEquals(8, record1.getPartition());
        assertEquals(150326091, record1.getOffset());
        // etc. check more fields if you like
    }

    @Test
    void testMainLambdaHandlerWithSampleJson() throws Exception {
        KafkaEvent event = TestUtils.buildKafkaEventFromJson();
        MainLambdaHandler handler = new MainLambdaHandler();

        String result = handler.handleRequest(event, null);

        // assertions, etc.
        System.out.println("Lambda result: " + result);
    }

}
