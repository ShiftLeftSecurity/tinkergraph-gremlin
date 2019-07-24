package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Contains all static node-specific information. This could in theory be part of OverflowDbNode, but to save memory
 * we need to minimize the amount of fields per node instance. While there may be millions of node instances, there's
 * only one NodeLayoutInformation instance per node type, which holds this static information.
 * <p>
 * Please make sure to instantiate only one instance per node type to not waste memory.
 */
public class NodeLayoutInformation {
  private final Set<String> propertyKeys;
  private final Map<String, Integer> outEdgeToOffsetPosition;
  private final Map<String, Integer> inEdgeToOffsetPosition;
  private final Map<String, HashSet<String>> edgePropertyKeysByLabel;
  private final Map<String, Integer> edgeLabelAndKeyToPosition;

  private final String[] allowedOutEdgeLabels;
  private final String[] allowedInEdgeLabels;

  /**
   * @param propertyKeys              possible property key names, e.g. ["NAME", "AGE"]
   * @param outEdgeToOffsetPosition   position for given OUT edge label in edgeOffsets
   * @param inEdgeToOffsetPosition    position for given IN edge label in edgeOffsets
   * @param edgePropertyKeysByLabel   possible edge property keys, grouped by edge label.
   *                                  n.b. property keys are of type `HashSet` (rather than just `Set`) to ensure `.size` has constant time
   * @param edgeLabelAndKeyToPosition position in edge offsets helper array
   */
  public NodeLayoutInformation(Set<String> propertyKeys,
                               Map<String, Integer> outEdgeToOffsetPosition,
                               Map<String, Integer> inEdgeToOffsetPosition,
                               Map<String, HashSet<String>> edgePropertyKeysByLabel,
                               Map<String, Integer> edgeLabelAndKeyToPosition) {
    this.propertyKeys = propertyKeys;
    this.outEdgeToOffsetPosition = outEdgeToOffsetPosition;
    this.inEdgeToOffsetPosition = inEdgeToOffsetPosition;
    this.edgePropertyKeysByLabel = edgePropertyKeysByLabel;
    this.edgeLabelAndKeyToPosition = edgeLabelAndKeyToPosition;

    /* only so we don't need to calculate it every time */
    allowedOutEdgeLabels = outEdgeToOffsetPosition.keySet().toArray(new String[0]);
    allowedInEdgeLabels = inEdgeToOffsetPosition.keySet().toArray(new String[0]);
  }

  /* property keys for this node */
  public Set<String> propertyKeys() {
    return propertyKeys;
  }

  public String[] allowedOutEdgeLabels() {
    return allowedOutEdgeLabels;
  }

  public String[] allowedInEdgeLabels() {
    return allowedInEdgeLabels;
  }

  public Set<String> edgePropertyKeys(String edgeLabel) {
    return edgePropertyKeysByLabel.get(edgeLabel);
  }

  /* The number fo different IN|OUT edge relations. E.g. a node has AST edges in and out, then we would have 2.
   * If in addition it has incoming ref edges it would have 3. */
  public int numberOfDifferentAdjacentTypes() {
    return outEdgeToOffsetPosition.size() + inEdgeToOffsetPosition.size();
  }

  /* position for given OUT edge label in OverflowDbNode.edgeOffsets */
  public Integer outEdgeToOffsetPosition(String edgeLabel) {
    return outEdgeToOffsetPosition.get(edgeLabel);
  }

  /* position for given IN edge label in OverflowDbNode.edgeOffsets */
  public Integer inEdgeToOffsetPosition(String edgeLabel) {
    return inEdgeToOffsetPosition.get(edgeLabel);
  }

  /**
   * @return The offset relative to the adjacent vertex element in the
   * adjacentVerticesWithProperties array starting from 1. Return -1 if
   * key does not exist for given edgeLabel.
   */
  public int getOffsetRelativeToAdjacentVertexRef(String edgeLabel, String key) {
    return edgeLabelAndKeyToPosition.getOrDefault(edgeLabel + key, -1);
  }

}

