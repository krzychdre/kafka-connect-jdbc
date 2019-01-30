package io.confluent.connect.jdbc.sink.transformer;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

public class SinkRecordTransformerManagerTest {

  @Test
  public void omitTransformingTombstone() {
    String topic = "books";

    SinkRecordTransformerManager transformerManager = new SinkRecordTransformerManager();

    transformerManager.addTransformer(new KafkaTimestampAppender(), true);
    transformerManager.addTransformer(new KafkaOffsetAppender(), true);

    SinkRecord tombStone = new SinkRecord(topic, 0, null, null, null,
        null,  0L, 1231233434L, TimestampType.CREATE_TIME);

    SinkRecord record = transformerManager.transform(tombStone);

    assertTrue(record.equals(tombStone));
  }

}