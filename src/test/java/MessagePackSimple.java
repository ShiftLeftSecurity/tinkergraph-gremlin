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
import org.apache.commons.lang3.NotImplementedException;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessagePackSimple {

  public static void main(String[] args) throws IOException {
    MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
//    packer
//      .packInt(1)
//      .packString("leo")
//      .packLong(Long.MAX_VALUE)
//      .packArrayHeader(2)
//      .packString("xxx-xxxx")
//      .packString("yyy-yyyy");

//    Map<String, Object> m = new HashMap();
//    m.put("one", Long.MAX_VALUE);
//    m.put("two", true);
//    m.put("three", 3);
//    packer.packMapHeader(m.size());
//    m.forEach((key, value) -> {
//      try {
//        packer.packString(key);
//        if (value.getClass() == Long.class) packer.packLong((Long) value);
//        else if (value.getClass() == Integer.class) packer.packInt((int) value);
//        else if (value.getClass() == Boolean.class) packer.packBoolean((Boolean) value);
//        else throw new NotImplementedException("type not yet supported: " + value.getClass());
//      } catch (IOException e) {
//        e.printStackTrace();
//      }
//    });

//    Map<String, long[]> edgeIdsByLabel = new HashMap();

    packer.packMapHeader(2);

    packer.packString("label1__OUT");
    packer.packArrayHeader(2);
    packer.packLong(99);
    packer.packLong(98);

    packer.packString("label2__IN_");
    packer.packArrayHeader(1);
    packer.packLong(99);


//    packer
//      .packString("start")
//      .packMapHeader(2)
//      .packString("one").packLong(Long.MAX_VALUE)
//      .packString("two").packBoolean(true)
//      .packString("end");
    packer.close();

    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(packer.toByteArray());

    //    int id = unpacker.unpackInt();             // 1
//    String name = unpacker.unpackString();     // "leo"
//    int numPhones = unpacker.unpackArrayHeader();  // 2
//    String[] phones = new String[numPhones];
//    for (int i = 0; i < numPhones; ++i) {
//      phones[i] = unpacker.unpackString();   // phones = {"xxx-xxxx", "yyy-yyyy"}
//    }
//    System.out.println(String.format("id:%d, name:%s, phone:[%s]", id, name, join(phones)));

//      MessageFormat format = unpacker.getNextFormat();
    while (unpacker.hasNext()) {
      Value v = unpacker.unpackValue();
      System.out.println(v);
      System.out.println(v.getValueType());
    }

    unpacker.close();
  }

}
