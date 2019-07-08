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

  protected abstract void storeAdjacentNode(String edgeLabel, Direction direction, VertexRef<OverflowDbNode> nodeRef);
  /** handle only IN|OUT direction, not BOTH */
  protected abstract Iterator<Vertex> adjacentVertices(Direction direction, String edgeLabel);
  /** handle only IN|OUT direction, not BOTH */
  protected abstract Iterator<Edge> adjacentDummyEdges(Direction direction, String edgeLabel);

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
    return instantiateDummyEdge(label, thisVertexRef, inVertexRef);
  }

  @Override
  public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
    final Labels labels = calculateInOutLabelsToFollow(direction, edgeLabels);
    final MultiIterator2<Edge> multiIterator = new MultiIterator2<>();
    for (String label : labels.forInEdges) {
      multiIterator.addIterator(adjacentDummyEdges(Direction.IN, label));
    }
    for (String label : labels.forOutEdges) {
      multiIterator.addIterator(adjacentDummyEdges(Direction.OUT, label));
    }

    return multiIterator;
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
    final Labels labels = calculateInOutLabelsToFollow(direction, edgeLabels);
    final MultiIterator2<Vertex> multiIterator = new MultiIterator2<>();
    for (String label : labels.forInEdges) {
      multiIterator.addIterator(adjacentVertices(Direction.IN, label));
    }
    for (String label : labels.forOutEdges) {
      multiIterator.addIterator(adjacentVertices(Direction.OUT, label));
    }

    return multiIterator;
  }

  private Labels calculateInOutLabelsToFollow(Direction direction, String... edgeLabels) {
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

    return new Labels(inEdgeLabels, outEdgeLabels);
  }

  /**  to follow the tinkerpop api, instantiate and return a dummy edge, which doesn't really exist in the graph */
  protected SpecializedTinkerEdge instantiateDummyEdge(String label, VertexRef<OverflowDbNode> outVertex, VertexRef<OverflowDbNode> inVertex) {
    final SpecializedElementFactory.ForEdge edgeFactory = graph.specializedEdgeFactoryByLabel.get(label);
    if (edgeFactory == null) throw new IllegalArgumentException("specializedEdgeFactory for label=" + label + " not found - please register on startup!");
    return edgeFactory.createEdge(-1l, graph, outVertex, inVertex);
  }

  private class Labels {
    private final Set<String> forInEdges;
    private final Set<String> forOutEdges;

    public Labels(Set<String> forInEdges, Set<String> forOutEdges) {
      this.forInEdges = forInEdges;
      this.forOutEdges = forOutEdges;
    }
  }
}
