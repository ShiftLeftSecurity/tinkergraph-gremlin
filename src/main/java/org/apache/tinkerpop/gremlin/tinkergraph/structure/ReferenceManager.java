package org.apache.tinkerpop.gremlin.tinkergraph.structure;

public interface ReferenceManager {
  void registerRef(ElementRef ref);

  void applyBackpressureMaybe();

  void close();
}
