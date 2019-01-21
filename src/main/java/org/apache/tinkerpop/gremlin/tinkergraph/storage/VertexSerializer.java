package org.apache.tinkerpop.gremlin.tinkergraph.storage;

import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.util.Map;

public class VertexSerializer extends Serializer<SpecializedTinkerVertex> {

  protected final TinkerGraph graph;
  protected final Map<String, SpecializedElementFactory.ForVertex> vertexFactoryByLabel;

  public VertexSerializer(TinkerGraph graph, Map<String, SpecializedElementFactory.ForVertex> vertexFactoryByLabel) {
    this.graph = graph;
    this.vertexFactoryByLabel = vertexFactoryByLabel;
  }

  @Override
  public byte[] serialize(SpecializedTinkerVertex vertex) throws IOException {
    MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
    packer.packLong((Long) vertex.id());
    packer.packString(vertex.label());
    packProperties(packer, vertex.properties());

    return packer.toByteArray();
  }

  @Override
  public SpecializedTinkerVertex deserialize(byte[] bytes) throws IOException {
    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes);
    Long id = unpacker.unpackLong();
    String label = unpacker.unpackString();
    Object[] keyValues = unpackProperties(unpacker.unpackValue().asMapValue().map());

    SpecializedTinkerVertex vertex = vertexFactoryByLabel.get(label).createVertex(id, graph);
    ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);

    return vertex;
  }

}
