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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public class BaseKafkaFieldAppender {

  public SchemaBuilder rewriteAllFields(SinkRecord record) {
    SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct();
    for (Field field : record.valueSchema().fields()) {
      valueSchemaBuilder.field(field.name(), field.schema());
    }

    return valueSchemaBuilder;
  }

  public Struct rewriteAllFieldsValues(SinkRecord record, Schema newRecordSchema) {
    Struct val = (Struct) record.value();
    Struct recordValues = new Struct(newRecordSchema);

    for (Field field : newRecordSchema.fields()) {
      try {
        recordValues.put(field, val.get(field.name()));
      } catch (DataException ex) {
        //newly created field has no data in original record
      }
    }
    return recordValues;
  }


  public void setFieldValue(Struct recordValues, String fieldName, Object value) {
    recordValues.put(fieldName, value);
  }
}
