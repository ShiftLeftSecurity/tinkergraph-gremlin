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
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.ElementRef;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class Serializer<A extends Element> {

  public abstract byte[] serialize(A a) throws IOException;
  public abstract A deserialize(byte[] bytes) throws IOException;

  /** only deserialize the part we're keeping in memory, used during startup when initializing from disk */
  public abstract ElementRef deserializeRef(byte[] bytes) throws IOException;

  /** when deserializing, msgpack can't differentiate between e.g. int and long, so we need to encode the type as well - doing that with an array
   *  i.e. format is: Map[PropertyName, Array(TypeId, PropertyValue)]
   * */
  protected void packProperties(MessageBufferPacker packer, Map<String, Object> properties) throws IOException {
    packer.packMapHeader(properties.size());
    for (Map.Entry<String, Object> property : properties.entrySet()) {
      packer.packString(property.getKey());
      packPropertyValue(packer, property.getValue());
    }
  }

  protected void packProperties(MessageBufferPacker packer, Iterator<? extends Property> propertyIterator) throws IOException {
    Map<String, Object> properties = new HashMap<>();
    while (propertyIterator.hasNext()) {
      Property property = propertyIterator.next();
      properties.put(property.key(), property.value());
    }
    packProperties(packer, properties);
  }

  private void packPropertyValue(final MessageBufferPacker packer, final Object value) throws IOException {
    if (value instanceof Boolean ) {
      packer.packBoolean((Boolean) value);
    } else if (value instanceof String) {
      packer.packString((String) value);
    } else if (value instanceof Byte) {
      packer.packByte((byte) value);
    } else if (value instanceof Short) {
      packer.packShort((short) value);
    } else if (value instanceof Integer) {
      packer.packInt((int) value);
    } else if (value instanceof Long) {
      packer.packLong((long) value);
    } else if (value instanceof Float || value instanceof Double) {
      packer.packFloat((float) value);
    } else if (value instanceof List) {
      List listValue = (List) value;
      packer.packArrayHeader(listValue.size());
      final Iterator listIter = listValue.iterator();
      while (listIter.hasNext()) {
        packPropertyValue(packer, listIter.next());
      }
    } else {
      throw new NotImplementedException("value type `" + value.getClass() + "` not yet supported");
    }
  }

  protected Object[] unpackProperties(Map<Value, Value> properties) {
    Object[] keyValues = new Object[properties.size() * 2];
    int idx = 0;
    for (Map.Entry<Value, Value> entry : properties.entrySet()) {
      String key = entry.getKey().asStringValue().asString();
      keyValues[idx++] = key;
      keyValues[idx++] = unpackProperty(entry.getValue());
    }
    return keyValues;
  }

  private Object unpackProperty(final Value packedValue) {
    switch (packedValue.getValueType()) {
      case BOOLEAN:
        return packedValue.asBooleanValue().getBoolean();
      case STRING:
        return packedValue.asStringValue().asString();
      case INTEGER:
        final IntegerValue integerValue = packedValue.asIntegerValue();
        if (integerValue.isInByteRange()) {
          return integerValue.asByte();
        } else if (integerValue.isInShortRange()) {
          return integerValue.asShort();
        } else if (integerValue.isInIntRange()) {
          return integerValue.asInt();
        } else if (integerValue.isInLongRange()) {
          return integerValue.asLong();
        } else {
          return integerValue.asBigInteger();
        }
      case FLOAT:
        return packedValue.asFloatValue().toFloat();
      case ARRAY:
        final ArrayValue arrayValue = packedValue.asArrayValue();
        List deserializedArray = new ArrayList(arrayValue.size());
        final Iterator<Value> valueIterator = arrayValue.iterator();
        while (valueIterator.hasNext()) {
          deserializedArray.add(unpackProperty(valueIterator.next()));
        }
        return deserializedArray;
      default:
        throw new NotImplementedException("type  `" + packedValue.getValueType() + "` not yet supported (packedValue=" + packedValue + ")");
    }
  }

}
