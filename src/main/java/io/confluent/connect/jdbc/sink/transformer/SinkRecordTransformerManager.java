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

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SinkRecordTransformerManager implements SinkRecordTransformer {

  private List<SinkRecordTransformer> transformerList = new ArrayList<>();

  public SinkRecordTransformerManager addTransformer(
      SinkRecordTransformer transformer,
      boolean enabled
  ) {
    if (enabled) {
      transformerList.add(transformer);
    }

    return this;
  }

  @Override
  public SinkRecord transform(SinkRecord record) {
    Iterator<SinkRecordTransformer> transformerIterator = transformerList.iterator();
    while (transformerIterator.hasNext()) {
      record = transformerIterator.next().transform(record);
    }

    return record;
  }

}
