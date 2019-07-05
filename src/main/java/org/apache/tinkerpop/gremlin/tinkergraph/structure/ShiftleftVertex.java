package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public abstract class ShiftleftVertex implements Vertex {
  private final Object id;
  private final ShiftleftGraph graph;

  public ShiftleftVertex(Object id, ShiftleftGraph graph) {
    this.id = id;
    this.graph = graph;
  }

  public abstract List<String> specificKeys();
  protected abstract <V> Iterator<VertexProperty<V>> specificProperties(String key);
  protected abstract <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value);

  public abstract <V> Property<V> edgeProperty(ShiftleftVertex inVertex,
                                               String label,
                                               String key,
                                               V value);

  public abstract void removeOutEdge(ShiftleftVertex inVertex, String label);
  public abstract void removeInEdge(ShiftleftVertex outVertex, String label);

  public abstract <V> Iterator<Property<V>> edgeProperties(ShiftleftVertex inVertex,
                                                           String label,
                                                           String... propertyKeys);

  @Override
  public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
    return null;
  }

  @Override
  public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
    if (keyValues.length > 0) {
      throw new RuntimeException("Not supported.");
    }
    VertexProperty<V> property = updateSpecificProperty(cardinality, key, value);
    graph.updateIndex(key, value, this);
    return property;
  }

  @Override
  public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
    return null;
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
    return null;
  }

  @Override
  public Object id() {
    return id;
  }

  @Override
  public Graph graph() {
    return graph;
  }

  @Override
  public void remove() {
    graph.removeVertex(this);
  }

  @Override
  public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
    if (propertyKeys.length == 0) { // return all properties
      return specificKeys().stream().flatMap(key ->
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(
              this.<V>specificProperties(key), Spliterator.ORDERED),false)
      ).iterator();
    } else if (propertyKeys.length == 1) { // treating as special case for performance
      return specificProperties(propertyKeys[0]);
    } else {
      return Arrays.stream(propertyKeys).flatMap(key ->
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(
              this.<V>specificProperties(key), Spliterator.ORDERED),false)
      ).iterator();
    }
  }
}
