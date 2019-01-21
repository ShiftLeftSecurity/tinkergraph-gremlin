package org.apache.tinkerpop.gremlin.tinkergraph.storage;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.Value;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

public class DefaultVertexSerializer implements Serializer<SpecializedTinkerVertex> {

  protected final TinkerGraph graph;
  protected final Map<String, SpecializedElementFactory.ForVertex> vertexFactoryByLabel;

  public DefaultVertexSerializer(TinkerGraph graph, Map<String, SpecializedElementFactory.ForVertex> vertexFactoryByLabel) {
    this.graph = graph;
    this.vertexFactoryByLabel = vertexFactoryByLabel;
  }

  @Override
  public byte[] serialize(SpecializedTinkerVertex vertex) throws IOException {
    MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
    packer.packLong((Long) vertex.id());
    packer.packString(vertex.label());

    Iterator<VertexProperty<Object>> propertyIterator = vertex.properties();
    LinkedList<VertexProperty> properties = new LinkedList<>();
    while (propertyIterator.hasNext()) {
      properties.add(propertyIterator.next());
    }

    packer.packMapHeader(properties.size());
    for (Property property : properties) {
      packer.packString(property.key());
      Object value = property.value();
      if (value.getClass() == Long.class) packer.packLong((Long) value);
      else if (value.getClass() == Integer.class) packer.packInt((int) value);
      else if (value.getClass() == Boolean.class) packer.packBoolean((Boolean) value);
      else if (value.getClass() == String.class) packer.packString((String) value);
      else throw new NotImplementedException("value type `" + value.getClass() + "` not yet supported (key=" + property.key() + ")");
    }

    return packer.toByteArray();
  }

  @Override
  public SpecializedTinkerVertex deserialize(byte[] bytes) throws IOException {
    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes);
    Long id = unpacker.unpackLong();
    String label = unpacker.unpackString();

    Map<Value, Value> properties = unpacker.unpackValue().asMapValue().map();
    Object[] keyValues = new Object[properties.size() * 2];
    int idx = 0;
    for (Map.Entry<Value, Value> entry : properties.entrySet()) {
      String key = entry.getKey().asStringValue().asString();
      keyValues[idx++] = key;

      final Object value;
      Value packedValue = entry.getValue();

      if (packedValue.isBooleanValue()) value = packedValue.asBooleanValue().getBoolean();
      else if (packedValue.isStringValue()) value = packedValue.asStringValue().asString();
      else if (packedValue.isIntegerValue()) {
        IntegerValue integerValue = packedValue.asIntegerValue();
        if (integerValue.isInLongRange()) value = integerValue.asLong();
        else if (integerValue.isInIntRange()) value = integerValue.asInt();
        else if (integerValue.isInShortRange()) value = integerValue.asShort();
        else if (integerValue.isInByteRange()) value = integerValue.asByte();
        else throw new AssertionError("integerValue not in expected ranges: " + integerValue);
      } else throw new NotImplementedException("value type `" + packedValue.getValueType() + "` not yet supported (key=" + key + ")");

      keyValues[idx++] = value;
    }

    SpecializedTinkerVertex vertex = vertexFactoryByLabel.get(label).createVertex(id, graph);
    ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);

    return vertex;
  }
}
