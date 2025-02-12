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

        assertTrue(event.getRecords().containsKey("o2o.store.1-6"));
        assertEquals(2, event.getRecords().get("o2o.store.1-6").size());

        KafkaEvent.KafkaEventRecord record1 = event.getRecords().get("o2o.store.1-6").get(0);
        assertEquals("o2o.store.1", record1.getTopic());
        assertEquals(8, record1.getPartition());
        assertEquals(150326091, record1.getOffset());
    }
}
