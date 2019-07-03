package org.apache.tinkerpop.gremlin.tinkergraph.structure;

public class MandatoryPropertyUndefinedException extends RuntimeException {
  public MandatoryPropertyUndefinedException(long id, String label, String key) {
    super("key=" + key + ",label=" + label + ",id=" + id);
  }
}