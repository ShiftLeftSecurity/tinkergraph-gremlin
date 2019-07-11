package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class OverflowDbEdge extends SpecializedTinkerEdge {
  private final TinkerGraph graph;
  private final String label;
  private final VertexRef<OverflowDbNode> outVertex;
  private final VertexRef<OverflowDbNode> inVertex;

  public OverflowDbEdge(TinkerGraph graph,
                        String label,
                        VertexRef<OverflowDbNode> outVertex,
                        VertexRef<OverflowDbNode> inVertex, Set<String> specificKeys) {
    super(graph, -1L, outVertex, label, inVertex, specificKeys);
    this.graph = graph;
    this.label = label;
    this.outVertex = outVertex;
    this.inVertex = inVertex;
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction) {
    switch (direction) {
      case OUT:
        return IteratorUtils.of(outVertex);
      case IN:
        return IteratorUtils.of(inVertex);
      default:
        return IteratorUtils.of(outVertex, inVertex);
    }
  }

  @Override
  public Object id() {
    return -1;
  }

  @Override
  public String label() {
    return label;
  }

  @Override
  protected <V> Property<V> specificProperty(String key) {
    throw new RuntimeException("Not supported.");
  }

  @Override
  public Graph graph() {
    return graph;
  }

  @Override
  public <V> Property<V> property(String key, V value) {
    // TODO check if it's an allowed property key
    return outVertex.get().setEdgeProperty(label, key, value, inVertex, this);
  }

  @Override
  protected <V> Property<V> updateSpecificProperty(String key, V value) {
    throw new RuntimeException("Not supported.");
  }

  @Override
  protected void removeSpecificProperty(String key) {
    throw new RuntimeException("Not supported.");
  }

  @Override
  public void remove() {
    throw new RuntimeException("Not supported.");
  }

  @Override
  public <V> Iterator<Property<V>> properties(String... propertyKeys) {
    return outVertex.get().getEdgeProperty(label, inVertex, propertyKeys);
  }

  public <V> Property<V> property(String propertyKey) {
    return outVertex.get().getEdgeProperty(label, propertyKey, inVertex);
  }

}
