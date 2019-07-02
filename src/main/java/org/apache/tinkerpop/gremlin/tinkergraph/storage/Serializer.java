/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.storage;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.TLongSet;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public abstract class Serializer<A> {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private int serializedCount = 0;
  private long serializationTimeSpentMillis = 0;

  protected abstract long getId(A a);
  protected abstract String getLabel(A a);

  /**
   * Map<PropertyIndex, PropertyValue>, sorted by it's index so we can write it efficiently
   */
  protected abstract SortedMap<Integer, Object> getProperties(A a);

  protected abstract Map<String, TLongSet> getEdgeIds(A a, Direction direction);

  public byte[] serialize(A a) throws IOException {
    long start = System.currentTimeMillis();
    try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
      packer.packLong(getId(a));
      packer.packString(getLabel(a));

      packEdgeIds(packer, getEdgeIds(a, Direction.IN));
      packEdgeIds(packer, getEdgeIds(a, Direction.OUT));
      packProperties(packer, getProperties(a));

      serializedCount++;
      serializationTimeSpentMillis += System.currentTimeMillis() - start;
      if (serializedCount % 100000 == 0) {
        float avgSerializationTime = serializationTimeSpentMillis / (float) serializedCount;
        logger.debug("stats: serialized " + serializedCount + " instances in total (avg time: " + avgSerializationTime + "ms)");
      }
      return packer.toByteArray();
    }
  }

  /**
   * packing as Array[PropertyValue] - each property is identified by it's index
   */
  private void packProperties(MessageBufferPacker packer, SortedMap<Integer, Object> properties) throws IOException {
    packer.packArrayHeader(properties.size());
    int currentIdx = 0;
    for (Map.Entry<Integer, Object> property : properties.entrySet()) {
      // to ensure we write the properties with correct index, fill the void with Nil values
      final Integer propertyIdx = property.getKey();
      while (propertyIdx < currentIdx) {
        packer.packNil();
      }

      packPropertyValue(packer, property.getValue());
      currentIdx++;
    }
  }

  /**
   * writing just the value itself
   * every element type hard-codes the index and type for each property, so it can be deserialized again
   */
  private void packPropertyValue(final MessageBufferPacker packer, final Object value) throws IOException {
    if (value instanceof Boolean) {
      packer.packBoolean((Boolean) value);
    } else if (value instanceof String) {
      packer.packString((String) value);
    } else if (value instanceof Byte) {
      packer.packByte((byte) value);
    } else if (value instanceof Short) {
      packer.packShort((short) value);
    } else if (value instanceof Integer) {
      packer.packInt((int) value);
    } else if (value instanceof Long) {
      packer.packLong((long) value);
    } else if (value instanceof Float) {
      packer.packFloat((float) value);
    } else if (value instanceof Double) {
      packer.packFloat((float) value); //msgpack doesn't support double, but we still want to deserialize it as a double later
    } else if (value instanceof List) {
      List listValue = (List) value;
      packer.packArrayHeader(listValue.size());
      final Iterator listIter = listValue.iterator();
      while (listIter.hasNext()) {
        packPropertyValue(packer, listIter.next());
      }
    } else {
      throw new NotImplementedException("id type `" + value.getClass() + "` not yet supported");
    }
  }

  /**
   * format: two `Map<Label, Array<EdgeId>>`, i.e. one Map for `IN` and one for `OUT` edges
   */
  private void packEdgeIds(final MessageBufferPacker packer,
                           final Map<String, TLongSet> edgeIdsByLabel) throws IOException {
    packer.packMapHeader(edgeIdsByLabel.size());
    for (Map.Entry<String, TLongSet> entry : edgeIdsByLabel.entrySet()) {
      final String label = entry.getKey();
      packer.packString(label);
      final TLongSet edgeIds = entry.getValue();
      packer.packArrayHeader(edgeIds.size());
      final TLongIterator edgeIdIter = edgeIds.iterator();
      while (edgeIdIter.hasNext()) {
        packer.packLong(edgeIdIter.next());
      }
    }
  }

}
