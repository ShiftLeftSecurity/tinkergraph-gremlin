package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class ShiftleftEdge implements Edge {
  private final ShiftleftVertex outVertex;
  private final ShiftleftVertex inVertex;
  private final String label;

  public ShiftleftEdge(ShiftleftVertex outVertex,
                       ShiftleftVertex inVertex,
                       String label) {
    this.outVertex = outVertex;
    this.inVertex = inVertex;
    this.label = label;
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction) {
    switch (direction) {
      case OUT:
        return IteratorUtils.of(outVertex);
      case IN:
        return IteratorUtils.of(inVertex);
      default:
        return IteratorUtils.of(this.outVertex(), this.inVertex());
    }
  }

  @Override
  public Object id() {
    return this;
  }

  @Override
  public String label() {
    return label;
  }

  @Override
  public Graph graph() {
    return outVertex.graph();
  }

  @Override
  public <V> Property<V> property(String key, V value) {
    return outVertex.edgeProperty(inVertex, label(), key, value);
  }

  @Override
  public void remove() {
    outVertex.removeOutEdge(inVertex, label());
    inVertex.removeInEdge(outVertex, label());
  }

  @Override
  public <V> Iterator<Property<V>> properties(String... propertyKeys) {
    return outVertex.edgeProperties(inVertex, label(), propertyKeys);
  }
}
