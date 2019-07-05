package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import gnu.trove.map.hash.THashMap;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

public class ShiftleftGraph implements Graph {
  private final THashMap<Object, SpecializedTinkerVertex> vertices = new THashMap<>();

  public void removeVertex(ShiftleftVertex vertex) {
    //TODO
  }

  public void updateIndex(String key, Object value, ShiftleftVertex vertex) {
    //TODO
  }


  @Override
  public Vertex addVertex(Object... keyValues) {
    return null;
  }

  @Override
  public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
    return null;
  }

  @Override
  public GraphComputer compute() throws IllegalArgumentException {
    return null;
  }

  @Override
  public Iterator<Vertex> vertices(Object... vertexIds) {
    return null;
  }

  @Override
  public Iterator<Edge> edges(Object... edgeIds) {
    return null;
  }

  @Override
  public Transaction tx() {
    return null;
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public Variables variables() {
    return null;
  }

  @Override
  public Configuration configuration() {
    return null;
  }
}
