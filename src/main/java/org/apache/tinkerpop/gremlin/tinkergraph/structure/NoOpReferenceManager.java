package org.apache.tinkerpop.gremlin.tinkergraph.structure;

public class NoOpReferenceManager implements ReferenceManager {
  @Override
  public void registerRef(ElementRef ref) {
  }

  @Override
  public void applyBackpressureMaybe() {
  }

  @Override
  public void close() {
  }
}
