import java.io.File;
import java.io.IOException;

public class dstore2 {
    public static void main(String[] args) throws IOException {

        File folder = new File("store2");
        if (!folder.exists()) {
            folder.mkdir();
        } else if (!folder.isDirectory()) {
            System.err.println("Invalid target directory");
            System.exit(1);
        }

        try {
            Dstore store = new Dstore(6667, 12345, 60*1000, folder);
            store.listen();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
