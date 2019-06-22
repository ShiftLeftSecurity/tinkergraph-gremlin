/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.storage;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.ElementRef;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class Deserializer<A> {

  public abstract A deserialize(byte[] bytes) throws IOException;

  /** only deserialize the part we're keeping in memory, used during startup when initializing from disk */
  public abstract ElementRef deserializeRef(byte[] bytes) throws IOException;

  protected List unpackProperties(Map<Value, Value> properties) {
    List keyValues = new ArrayList(properties.size() * 2); // may grow bigger if there's list entries
    for (Map.Entry<Value, Value> entry : properties.entrySet()) {
      String key = entry.getKey().asStringValue().asString();
      final Object unpackedProperty = unpackProperty(entry.getValue().asArrayValue());
      // special handling for lists: create separate key/value entry for each list entry
      if (unpackedProperty instanceof List) {
        for (Object value : (List) unpackedProperty) {
          keyValues.add(key);
          keyValues.add(value);
        }
      } else {
        keyValues.add(key);
        keyValues.add(unpackedProperty);
      }
    }
    return keyValues;
  }

  private Object unpackProperty(final ArrayValue packedValueAndType) {
    final Iterator<Value> iter = packedValueAndType.iterator();
    final byte valueTypeId = iter.next().asIntegerValue().asByte();
    final Value value = iter.next();

    switch (ValueTypes.lookup(valueTypeId)) {
      case BOOLEAN: return value.asBooleanValue().getBoolean();
      case STRING: return value.asStringValue().asString();
      case BYTE: return value.asIntegerValue().asByte();
      case SHORT: return value.asIntegerValue().asShort();
      case INTEGER: return value.asIntegerValue().asInt();
      case LONG: return value.asIntegerValue().asLong();
      case FLOAT: return value.asFloatValue().toFloat();
      case DOUBLE: return Double.valueOf(value.asFloatValue().toFloat());
      case LIST:
        final ArrayValue arrayValue = value.asArrayValue();
        List deserializedArray = new ArrayList(arrayValue.size());
        final Iterator<Value> valueIterator = arrayValue.iterator();
        while (valueIterator.hasNext()) {
          deserializedArray.add(unpackProperty(valueIterator.next().asArrayValue()));
        }
        return deserializedArray;
      default:
        throw new NotImplementedException("unknown valueTypeId=`" + valueTypeId);
    }
  }

}
