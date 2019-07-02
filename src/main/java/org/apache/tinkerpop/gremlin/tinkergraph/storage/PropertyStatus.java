package org.apache.tinkerpop.gremlin.tinkergraph.storage;

public enum PropertyStatus {
  /** not yet read from storage */
  Uninitialized,

  /** equivalent of null/nil/none */
  Undefined,

  /* normal property that has been read from storage */
  Defined
}
