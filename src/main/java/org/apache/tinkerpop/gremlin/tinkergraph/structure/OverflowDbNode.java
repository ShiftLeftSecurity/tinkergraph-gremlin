package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import gnu.trove.set.hash.THashSet;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.storage.iterator.MultiIterator2;

import java.util.Iterator;
import java.util.Set;

/**
 * Variant of SpecializedTinkerVertex that stores adjacent Nodes directly, rather than via edges.
 * Motivation: in many graph use cases, edges don't hold any properties and thus accounts for more memory and
 * traversal time than necessary
 *
 * TODO: extend Vertex (rather than SpecializedTinkerVertex)
 */
public abstract class OverflowDbNode extends SpecializedTinkerVertex {

  protected OverflowDbNode(long id, TinkerGraph graph) {
    super(id, graph);
  }

  protected abstract void storeAdjacentNode(String label, Direction direction, VertexRef<OverflowDbNode> nodeRef);
  protected abstract Iterator<Vertex> adjacentVertices(Direction direction, String edgeLabel);

  @Override
  public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
    if (keyValues.length > 0) throw new NotImplementedException("edge properties not yet supported");

    final VertexRef<OverflowDbNode> inVertexRef;
    if (inVertex instanceof VertexRef) inVertexRef = (VertexRef<OverflowDbNode>) inVertex;
    else inVertexRef = (VertexRef<OverflowDbNode>) graph.vertex((Long) inVertex.id());
    OverflowDbNode inVertexOdb = inVertexRef.get();
    VertexRef<OverflowDbNode> thisVertexRef = (VertexRef) graph.vertex((Long) id());

    storeAdjacentNode(label, Direction.OUT, inVertexRef);
    inVertexOdb.storeAdjacentNode(label, Direction.IN, thisVertexRef);

    // TODO implement: create dummy edge on the fly - lookup edge factory by label
    return null;
  }

  @Override
  public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
    // TODO implement: create edges on the fly
    throw new NotImplementedException("");
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
    final MultiIterator2<Vertex> multiIterator = new MultiIterator2<>();

    final Set<String> inEdgeLabels;
    final Set<String> outEdgeLabels;
    if (edgeLabels.length == 0) { // follow all labels
      switch (direction) {
        case IN:
          inEdgeLabels = allowedInEdgeLabels();
          outEdgeLabels = new THashSet<>(0);
          break;
        case OUT:
          inEdgeLabels = new THashSet<>(0);
          outEdgeLabels = allowedOutEdgeLabels();
          break;
        case BOTH:
          inEdgeLabels = allowedInEdgeLabels();
          outEdgeLabels = allowedOutEdgeLabels();
          break;
        default:
          throw new IllegalStateException("this will never happen - only needed to make the compiler happy");
      }
    } else { // follow specific labels
      inEdgeLabels = new THashSet<>();
      outEdgeLabels = new THashSet<>();
      for (String label : edgeLabels) {
        if (direction == Direction.IN || direction == Direction.BOTH) {
          inEdgeLabels.add(label);
        }
        if (direction == Direction.OUT || direction == Direction.BOTH) {
          outEdgeLabels.add(label);
        }
      }
    }

    for (String label : inEdgeLabels) {
      multiIterator.addIterator(adjacentVertices(Direction.IN, label));
    }
    for (String label : outEdgeLabels) {
      multiIterator.addIterator(adjacentVertices(Direction.OUT, label));
    }

    return multiIterator;
  }

}
