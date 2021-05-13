import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Main {

    public static void main(String[] args) throws IOException {
        Controller c = new Controller(12345, 2, 100 * 1000, 30*1000);
        HashMap<Integer, List<String>> stores = new HashMap<>();
        HashMap<String, Integer> files = new HashMap<>();

        /*for(int i =0; i<5; i++){
            stores.put(i, new ArrayList<>());
            files.put("FILE" + i, 0);
        }
         */
        stores.put(6666, new ArrayList<>());
        stores.put(6667, new ArrayList<>());
        stores.put(6668, new ArrayList<>());
        files.put("a", 0);
        files.put("b", 0);
        //System.out.println(files);
        //System.out.println("RESULT: " +c.generateBalancedConfig(stores, files, 2));

        c.listen();
    }
}
