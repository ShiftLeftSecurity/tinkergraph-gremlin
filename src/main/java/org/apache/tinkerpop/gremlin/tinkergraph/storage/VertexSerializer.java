package org.apache.tinkerpop.gremlin.tinkergraph.storage;

import org.apache.commons.collections.IteratorUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.util.HashMap;
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

  /** format: two `Map<Label, Array<EdgeId>>`, i.e. one Map for `IN` and one for `OUT` edges */
  private void packEdgeIds(MessageBufferPacker packer, SpecializedTinkerVertex vertex) throws IOException {
    for (Direction direction : new Direction[]{Direction.IN, Direction.OUT}) {
      List<Edge> edges = IteratorUtils.toList(vertex.edges(direction));
      // a simple group by would be nice, but java collections are still very basic apparently
      Set<String> labels = edges.stream().map(e -> e.label()).collect(Collectors.toSet());
      packer.packMapHeader(labels.size());

      for (String label : labels) {
        packer.packString(label);
        Set<Long> edgeIds = edges.stream().filter(e -> e.label().equals(label)).map(e -> (Long) e.id()).collect(Collectors.toSet());
        packer.packArrayHeader(edgeIds.size());
        for (Long edgeId : edgeIds) {
          packer.packLong(edgeId);
        }
      }
    }
  }

  @Override
  public SpecializedTinkerVertex deserialize(byte[] bytes) throws IOException {
    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes);
    Long id = unpacker.unpackLong();
    String label = unpacker.unpackString();
    Object[] keyValues = unpackProperties(unpacker.unpackValue().asMapValue().map());

    SpecializedTinkerVertex vertex = vertexFactoryByLabel.get(label).createVertex(id, graph);
    ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);


    Map<String, long[]> inEdgeIdsByLabel = unpackEdges(unpacker);
    Map<String, long[]> outEdgeIdsByLabel = unpackEdges(unpacker);

    inEdgeIdsByLabel.entrySet().stream().forEach(entry -> {
      String edgeLabel = entry.getKey();
      for (long edgeId : entry.getValue()) {
        SpecializedTinkerEdge edge = graph.edgeById(edgeId);
        // TODO pass ID/label only so we don't need to deserialize
        vertex.addSpecializedInEdge(edge);
      }
    });

    outEdgeIdsByLabel.entrySet().stream().forEach(entry -> {
      String edgeLabel = entry.getKey();
      for (long edgeId : entry.getValue()) {
        SpecializedTinkerEdge edge = graph.edgeById(edgeId);
        // TODO pass ID/label only so we don't need to deserialize
        vertex.addSpecializedOutEdge(edge);
      }
    });

    return vertex;
  }

  /** format: `Map<Label, Array<EdgeId>>` */
  private Map<String, long[]> unpackEdges(MessageUnpacker unpacker) throws IOException {
    int labelCount = unpacker.unpackMapHeader();
    Map<String, long[]> edgeIdsByLabel = new HashMap<>(labelCount);
    for (int i = 0; i < labelCount; i++) {
      String label = unpacker.unpackString();
      int edgeIdsCount = unpacker.unpackArrayHeader();
      long[] edgeIds = new long[edgeIdsCount];
      for (int j = 0; j < edgeIdsCount; j++) {
        edgeIds[j] = unpacker.unpackLong();
      }
      edgeIdsByLabel.put(label, edgeIds);
    }
    return edgeIdsByLabel;
  }

}
