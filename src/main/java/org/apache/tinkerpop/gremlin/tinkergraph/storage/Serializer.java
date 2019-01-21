package org.apache.tinkerpop.gremlin.tinkergraph.storage;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.value.FloatValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.Value;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

public abstract class Serializer<A> {

  public abstract byte[] serialize(A a) throws IOException;
  public abstract A deserialize(byte[] bytes) throws IOException;

  protected void packProperties(MessageBufferPacker packer, Iterator<? extends Property> propertyIterator) throws IOException {
    LinkedList<Property> properties = new LinkedList<>();
    while (propertyIterator.hasNext()) {
      properties.add(propertyIterator.next());
    }

    packer.packMapHeader(properties.size());
    for (Property property : properties) {
      packer.packString(property.key());
      Object value = property.value();
      if (value.getClass() == Boolean.class) packer.packBoolean((Boolean) value);
      else if (value.getClass() == String.class) packer.packString((String) value);
      else if (value.getClass() == Byte.class) packer.packByte((Byte) value);
      else if (value.getClass() == Short.class) packer.packShort((Short) value);
      else if (value.getClass() == Integer.class) packer.packInt((int) value);
      else if (value.getClass() == Long.class) packer.packLong((Long) value);
      else if (value.getClass() == Float.class) packer.packFloat((Float) value);
      else if (value.getClass() == Double.class) packer.packDouble((Double) value);
      else throw new NotImplementedException("value type `" + value.getClass() + "` not yet supported (key=" + property.key() + ")");
    }
  }

  protected Object[] unpackProperties(Map<Value, Value> properties) {
    Object[] keyValues = new Object[properties.size() * 2];
    int idx = 0;
    for (Map.Entry<Value, Value> entry : properties.entrySet()) {
      String key = entry.getKey().asStringValue().asString();
      keyValues[idx++] = key;

      final Object value;
      Value packedValue = entry.getValue();

      if (packedValue.isBooleanValue()) {
        value = packedValue.asBooleanValue().getBoolean();
      } else if (packedValue.isStringValue()) {
        value = packedValue.asStringValue().asString();
      } else if (packedValue.isIntegerValue()) {
        IntegerValue integerValue = packedValue.asIntegerValue();
        if (integerValue.isInLongRange()) value = integerValue.asLong();
        else if (integerValue.isInIntRange()) value = integerValue.asInt();
        else if (integerValue.isInShortRange()) value = integerValue.asShort();
        else if (integerValue.isInByteRange()) value = integerValue.asByte();
        else throw new AssertionError("integerValue not in expected ranges: " + integerValue);
      } else if (packedValue.isFloatValue()) {
        // no way to check if it's a double or a float...
        FloatValue floatValue = packedValue.asFloatValue();
        value = floatValue.toDouble();
      } else {
        throw new NotImplementedException("value type `" + packedValue.getValueType() + "` not yet supported (key=" + key + ")");
      }

      keyValues[idx++] = value;
    }
    return keyValues;
  }
}
