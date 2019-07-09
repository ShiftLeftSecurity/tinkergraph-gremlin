package org.apache.tinkerpop.gremlin.tinkergraph.structure;

/** since we're only emulating edges, we're storing the edge properties together with the vertex
 * we store them on both sides (SRC and DST), because edge properties are relatively rare, and to avoid extra roundtrips for deserialization

 * current format: storing the edgeKeyValues array.
 * TODO: store only the values with a given offset - base work is already available in michael/deserializer-optimisations
 * */
public class VertexWithEdgeProperties {
  final VertexRef<OverflowDbNode> nodeRef;
  final Object[] edgeKeyValues;

  public VertexWithEdgeProperties(final VertexRef<OverflowDbNode> nodeRef, final Object[] edgeKeyValues) {
    this.nodeRef = nodeRef;
    this.edgeKeyValues = edgeKeyValues;
  }
}
