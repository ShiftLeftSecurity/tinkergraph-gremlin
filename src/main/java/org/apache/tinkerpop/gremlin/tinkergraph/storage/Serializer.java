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
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerElement;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.FloatValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.Value;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

public abstract class Serializer<A extends Element> {

  public abstract byte[] serialize(A a) throws IOException;
  public abstract A deserialize(byte[] bytes) throws IOException;

  /** when deserializing, msgpack can't differentiate between e.g. int and long, so we need to encode the type as well - doing that with an array
   *  i.e. format is: Map[PropertyName, Array(TypeId, PropertyValue)]
   * */
  protected void packProperties(MessageBufferPacker packer, Iterator<? extends Property> propertyIterator) throws IOException {
    LinkedList<Property> properties = new LinkedList<>();
    while (propertyIterator.hasNext()) {
      properties.add(propertyIterator.next());
    }

    packer.packMapHeader(properties.size());
    for (Property property : properties) {
      packer.packString(property.key());
      Object value = property.value();

      packer.packArrayHeader(2);
      // encode their type as well - as is, we can't differentiate between int and long
      if (value.getClass() == Boolean.class) {
        packer.packShort((short) 1);
        packer.packBoolean((Boolean) value);
      } else if (value.getClass() == String.class) {
        packer.packShort((short) 2);
        packer.packString((String) value);
      } else if (value.getClass() == Byte.class) {
        packer.packShort((short) 3);
        packer.packByte((Byte) value);
      } else if (value.getClass() == Short.class) {
        packer.packShort((short) 4);
        packer.packShort((Short) value);
      } else if (value.getClass() == Integer.class) {
        packer.packShort((short) 5);
        packer.packInt((int) value);
      } else if (value.getClass() == Long.class) {
        packer.packShort((short) 6);
        packer.packLong((Long) value);
      } else if (value.getClass() == Float.class) {
        packer.packShort((short) 7);
        packer.packFloat((Float) value);
      } else if (value.getClass() == Double.class) {
        packer.packShort((short) 8);
        packer.packDouble((Double) value);
      } else if (value.getClass() == int[].class) {
        //TODO remove this dummy case again
      } else throw new NotImplementedException("value type `" + value.getClass() + "` not yet supported (key=" + property.key() + ")");
    }
  }

  protected Object[] unpackProperties(Map<Value, Value> properties) {
    Object[] keyValues = new Object[properties.size() * 2];
    int idx = 0;
    for (Map.Entry<Value, Value> entry : properties.entrySet()) {
      String key = entry.getKey().asStringValue().asString();
      keyValues[idx++] = key;

      ArrayValue typeAndValue = entry.getValue().asArrayValue();
      short type = typeAndValue.get(0).asIntegerValue().asShort();

      final Object value;
      Value packedValue = typeAndValue.get(1);

      switch (type) {
        case 1:
          value = packedValue.asBooleanValue().getBoolean();
          break;
        case 2:
          value = packedValue.asStringValue().asString();
          break;
        case 3:
          value = packedValue.asIntegerValue().asByte();
          break;
        case 4:
          value = packedValue.asIntegerValue().asShort();
          break;
        case 5:
          value = packedValue.asIntegerValue().asInt();
          break;
        case 6:
          value = packedValue.asIntegerValue().asLong();
          break;
        case 7:
          value = packedValue.asFloatValue().toFloat();
          break;
        case 8:
          value = packedValue.asFloatValue().toDouble();
          break;
        default:
          throw new NotImplementedException("type prefix `" + type + "` not yet supported (key=" + key + ", packedValue=" + packedValue + ")");
      }

      keyValues[idx++] = value;
    }
    return keyValues;
  }
}
