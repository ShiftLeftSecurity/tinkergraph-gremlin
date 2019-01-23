package org.apache.tinkerpop.gremlin.tinkergraph.storage;

import org.apache.commons.collections.IteratorUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    packEdgeIds(packer, vertex);

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
    unpackEdges(unpacker, vertex);

    return vertex;
  }


  /** format: two `Map<Label, Array<EdgeId>>`, i.e. one Map for `IN` and one for `OUT` edges */
  private void packEdgeIds(MessageBufferPacker packer, SpecializedTinkerVertex vertex) throws IOException {
    for (Direction direction : new Direction[]{Direction.IN, Direction.OUT}) {
      List<Edge> edges = IteratorUtils.toList(vertex.edges(direction));
      // a simple group by would be nice, but java collections are still very basic apparently
      Set<String> labels = edges.stream().map(e -> e.label()).collect(Collectors.toSet());
      packer.packMapHeader(labels.size());
//      if (vertex.id().toString().equals("340") && direction.equals(Direction.IN)) {
//        System.out.println("VertexSerializer.packEdgeIds 340. labels.size=" + labels.size());
//      }
      if (vertex.id().toString().equals("1")) {
        System.out.println("VertexSerializer.packEdgeIds 1. labels.size=" + labels.size());
      }
      for (String label : labels) {
        Set<Long> edgeIds = edges.stream().filter(e -> e.label().equals(label)).map(e -> (Long) e.id()).collect(Collectors.toSet());
        packer.packArrayHeader(edgeIds.size());
        for (Long edgeId : edgeIds) {
          packer.packLong(edgeId);
        }
      }
    }
  }

  /** format: two `Map<Label, Array<EdgeId>>`, i.e. one Map for `IN` and one for `OUT` edges */
  private void unpackEdges(MessageUnpacker unpacker, SpecializedTinkerVertex vertex) throws IOException {
    for (Direction direction : new Direction[]{Direction.IN, Direction.OUT}) {
      try {
        Map<Value, Value> valueMap = unpacker.unpackValue().asMapValue().map();
      } catch (Exception e) {
        System.out.println("VertexSerializer.unpackEdges");
      }
    }
  }

}
