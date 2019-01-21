package org.apache.tinkerpop.gremlin.tinkergraph.storage;

import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.util.Map;

public class EdgeSerializer extends Serializer<SpecializedTinkerEdge> {

  protected final TinkerGraph graph;
  protected final Map<String, SpecializedElementFactory.ForEdge> edgeFactoryByLabel;

  public EdgeSerializer(TinkerGraph graph, Map<String, SpecializedElementFactory.ForEdge> edgeFactoryByLabel) {
    this.graph = graph;
    this.edgeFactoryByLabel = edgeFactoryByLabel;
  }

  @Override
  public byte[] serialize(SpecializedTinkerEdge edge) throws IOException {
    MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
    packer.packLong((Long) edge.id());
    packer.packString(edge.label());
    packProperties(packer, edge.properties());
    packer.packLong(edge.outVertexId);
    packer.packLong(edge.inVertexId);

    return packer.toByteArray();
  }


  @Override
  public SpecializedTinkerEdge deserialize(byte[] bytes) throws IOException {
    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes);
    Long id = unpacker.unpackLong();
    String label = unpacker.unpackString();
    Object[] keyValues = unpackProperties(unpacker.unpackValue().asMapValue().map());
    long outVertexId = unpacker.unpackLong();
    long inVertexId = unpacker.unpackLong();

    SpecializedTinkerEdge edge = edgeFactoryByLabel.get(label).createEdge(id, graph, outVertexId, inVertexId);
    ElementHelper.attachProperties(edge, keyValues);

    return edge;
  }
}
