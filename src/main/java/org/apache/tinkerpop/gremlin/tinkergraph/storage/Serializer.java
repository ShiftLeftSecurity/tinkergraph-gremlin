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

  /** format: `[ValueType.id, value]` */
  private void packPropertyValue(final MessageBufferPacker packer, final Object value) throws IOException {
    packer.packArrayHeader(2);
    if (value instanceof Boolean) {
      packer.packByte(ValueTypes.BOOLEAN.id);
      packer.packBoolean((Boolean) value);
    } else if (value instanceof String) {
      packer.packByte(ValueTypes.STRING.id);
      packer.packString((String) value);
    } else if (value instanceof Byte) {
      packer.packByte(ValueTypes.BYTE.id);
      packer.packByte((byte) value);
    } else if (value instanceof Short) {
      packer.packByte(ValueTypes.SHORT.id);
      packer.packShort((short) value);
    } else if (value instanceof Integer) {
      packer.packByte(ValueTypes.INTEGER.id);
      packer.packInt((int) value);
    } else if (value instanceof Long) {
      packer.packByte(ValueTypes.LONG.id);
      packer.packLong((long) value);
    } else if (value instanceof Float) {
      packer.packByte(ValueTypes.FLOAT.id);
      packer.packFloat((float) value);
    } else if (value instanceof Double) {
      packer.packByte(ValueTypes.DOUBLE.id);
      packer.packFloat((float) value); //msgpack doesn't support double, but we still want to deserialize it as a double later
    } else if (value instanceof List) {
      packer.packByte(ValueTypes.LIST.id);
      List listValue = (List) value;
      packer.packArrayHeader(listValue.size());
      final Iterator listIter = listValue.iterator();
      while (listIter.hasNext()) {
        packPropertyValue(packer, listIter.next());
      }
    } else {
      throw new NotImplementedException("id type `" + value.getClass() + "` not yet supported");
    }
  }

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

  /* when serializing properties we need to encode the id type in a separate entry, to ensure we can deserialize it
   * back to the very same type. I would have hoped that MsgPack does that for us, but that's only partly the case.
   * E.g. the different integer types cannot be distinguished other than by their value. When we deserialize `42`,
   * we have no idea whether it should be deserialized as a byte, short, integer or double */
  public enum ValueTypes {
    BOOLEAN((byte) 0),
    STRING((byte) 1),
    BYTE((byte) 2),
    SHORT((byte) 3),
    INTEGER((byte) 4),
    LONG((byte) 5),
    FLOAT((byte) 6),
    DOUBLE((byte) 7),
    LIST((byte) 8);

    public final byte id;
    ValueTypes(byte id) {
      this.id = id;
    }

    public static ValueTypes lookup(byte id) {
      switch (id) {
        case 0: return BOOLEAN;
        case 1: return STRING;
        case 2: return BYTE;
        case 3: return SHORT;
        case 4: return INTEGER;
        case 5: return LONG;
        case 6: return FLOAT;
        case 7: return DOUBLE;
        case 8: return LIST;
        default: throw new IllegalArgumentException("unknown id type " + id);
      }
    }
  }
}
