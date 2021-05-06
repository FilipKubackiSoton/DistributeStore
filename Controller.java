
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Controller {
    private static int numAllStore;
    private static int _R;
    private static int _MAXCLIENTS = 50;
    private static int _MAXCSTORES = 50;
    private static int _MAXFILES = 500;
    public static boolean _readyToStore;
    private static int cport;
    private static int timeout;
    private static int rebalancePeriod;
    public static Map<String, PrintWriter> _mapClients = Collections.synchronizedMap(new HashMap<>(_MAXCLIENTS));
    public static Map<String, PrintWriter> _mapDstores = Collections.synchronizedMap(new HashMap<>(_MAXCLIENTS));
    public static Map<String, Integer> _mapDstoresPorts = Collections.synchronizedMap(new HashMap<>(_MAXCLIENTS));
    public static Set<String> _mapFiless = Collections.synchronizedSet(new HashSet(_MAXCLIENTS));

    public Controller(int cport, final int R, int timeout, int rebalancePeriod) {
        this.cport = cport;
        this._R = R;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;
        this.numAllStore = 0;
        try {
            ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void startListening() {
        try {
            ServerSocket ss = new ServerSocket(cport);
            for (;;) {
                try {
                    Socket client = ss.accept();
                    new Thread(new ControllerThread(client)).start();
                } catch (IOException e) {
                    System.out.println("Error inner loop: " + e.getMessage());
                    ss.close();
                }
            }
        } catch (IOException e) {
            System.out.println("Error outer loop: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        Controller controller = new Controller(8989, 3, 1000, 100);
        controller.startListening();
    }

    public static synchronized void storeTokenHanlder(StoreToken token) {

        System.out.println("Handeling StoreToken " + token._filename + " " + token._filesize);
        // out.println(Controller.getStoreToRequest());

    }

    public static synchronized void listTokenHandler(ListToken token) {
        System.out.println("Handeling ListToken");
    }

    public static synchronized boolean joinTokenHanlder(JoinToken token) {
        numAllStore++;
        String name = "dstore" + numAllStore;
        int port = token._port;

        if (_mapDstores.size() >= _MAXCSTORES)
            return false;
        if (_mapDstores.containsKey(name)) {
            System.err.println("Controller: Store of this name already joined.");
            return false;
        }

        try {
            createStoreDirectory(name);
        } catch (RuntimeException e) {
            return false;
        }

        try {
            Socket socket = new Socket("localhost", port);
            _mapDstores.put(name, new PrintWriter(socket.getOutputStream(), true));
            _mapDstoresPorts.put(name, port);
        } catch (NullPointerException e) {
            return false;
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return false;
        }

        _readyToStore = _mapDstores.size() >= _R;

        return true;
    }

    public static synchronized void createStoreDirectory(String storeName) throws RuntimeException {
        File downloadFolder = new File(storeName);
        if (!downloadFolder.exists())
            if (!downloadFolder.mkdir())
                throw new RuntimeException("Cannot create download folder (folder absolute path: "
                        + downloadFolder.getAbsolutePath() + ")");
    }

    public static synchronized String getStoreToRequest() {
        StringBuilder sb = new StringBuilder(Protocol.STORE_TO_TOKEN);
        int counter = 0;
        ArrayList<Integer> ports = (ArrayList<Integer>) _mapDstoresPorts.values();
        Collections.shuffle(ports);
        for (int port : ports) {
            sb.append(" ").append(port);
            counter++;
            if (counter == _R)
                break;
        }
        return sb.toString();
    }
}
