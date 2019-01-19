package org.apache.tinkerpop.gremlin.tinkergraph.storage;

import java.io.IOException;

public interface Serializer<A> {
  public byte[] serialize(A a) throws IOException;
  public A deserialize(byte[] bytes) throws IOException;
}
