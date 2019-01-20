import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

public class MVStoreSimple {

  public static void main(String[] args) {
    boolean insert = false;
    String fileName = "mvstore.bin";

    if (insert) {
      MVStore s = new MVStore.Builder().fileName(fileName).open();
      MVMap<Integer, String> map = s.openMap("data");
      map.put(1, "Hello World");
      s.close();
    } else {
        MVStore s = new MVStore.Builder().fileName(fileName).readOnly().open();

        MVMap<Integer, String> map = s.openMap("data");
        map.put(1, "Hello World");
        System.out.println(map.get(1));
        s.close();
    }
  }
}
