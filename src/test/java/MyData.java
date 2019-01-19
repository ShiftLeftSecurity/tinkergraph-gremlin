import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.collections.StoredKeySet;
import com.sleepycat.collections.StoredSortedKeySet;

public class MyData {

  public final long nodeId;

  public final String label;

  public MyData(long nodeId, String label) {
    this.nodeId = nodeId;
    this.label = label;
  }

  public static TupleBinding<MyData> tupleBinding = new TupleBinding<MyData>() {
    @Override
    public void objectToEntry(MyData o, TupleOutput to) {
      to.writeLong(o.nodeId);
      to.writeString(o.label);
    }

    @Override
    public MyData entryToObject(TupleInput ti) {
      long nodeId = ti.readLong();
      String label = ti.readString();
      return new MyData(nodeId, label);
    }
  };
}