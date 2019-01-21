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

    Map<String, Object> m = new HashMap();
    m.put("one", Long.MAX_VALUE);
    m.put("two", true);
    m.put("three", 3);
    packer.packMapHeader(m.size());
    m.forEach((key, value) -> {
      try {
        packer.packString(key);
        if (value.getClass() == Long.class) packer.packLong((Long) value);
        else if (value.getClass() == Integer.class) packer.packInt((int) value);
        else if (value.getClass() == Boolean.class) packer.packBoolean((Boolean) value);
        else throw new NotImplementedException("type not yet supported: " + value.getClass());
      } catch (IOException e) {
        e.printStackTrace();
      }
    });


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
