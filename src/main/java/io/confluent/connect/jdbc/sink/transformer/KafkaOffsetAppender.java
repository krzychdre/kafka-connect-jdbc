/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink.transformer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

public class KafkaOffsetAppender implements SinkRecordTransformer {

  public static final String KAFKA_OFFSET = "_kafka_offset";
  private final BaseKafkaFieldAppender baseAppender = new BaseKafkaFieldAppender();

  @Override
  public SinkRecord transform(SinkRecord record) {

    Schema recordSchema = baseAppender.rewriteAllFields(record)
        .field(KAFKA_OFFSET, Schema.OPTIONAL_INT64_SCHEMA).build();

    Struct recordValues = baseAppender.rewriteAllFieldsValues(record, recordSchema);
    baseAppender.setFieldValue(recordValues, KAFKA_OFFSET, record.kafkaOffset());

    return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(),
        record.key(), recordSchema, recordValues, record.timestamp());
  }
}
