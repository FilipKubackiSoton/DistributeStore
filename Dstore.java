
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class Dstore {
    private int port;
    private int cport;
    private int timeout;
    private String file_folder;
    private String name;

    public Dstore(int port, int cport, int timeout, String file_folder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.file_folder = file_folder;
        this.name = "dstore" + System.currentTimeMillis();
        try {
            DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, this.port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void startListening() {
        try {
            ServerSocket ss = new ServerSocket(port);
            for (;;) {
                try {
                    Socket dstore = ss.accept();
                    new Thread(new DstoreReadThread(dstore)).start();
                } catch (IOException e) {
                    System.out.println("Error inner loop: " + e.getMessage());
                    ss.close();
                }
            }
        } catch (IOException e) {
            System.out.println("Error outer loop: " + e.getMessage());
        }
    }

    public void testMsg(String msg) {
        try {
            Socket socket = new Socket("localhost", this.cport);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            for (int i = 0; i < 10; i++) {
                out.println(msg + i);
                DstoreLogger.getInstance().messageSent(socket, msg + i);
                System.out.println(msg + i);
                Thread.sleep(1000);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void tellController(String msg) {
        try {
            Socket socket = new Socket("localhost", this.cport);
            tellController(socket, msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void tellController(Socket socket, String msg) {
        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(msg);
            DstoreLogger.getInstance().messageSent(socket, msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void connectToController() {
        tellController(Protocol.JOIN_TOKEN + " " + this.port);
    }

    public void createStoreDirectory() {
        File downloadFolder = new File("storeName");
        if (!downloadFolder.exists())
            if (!downloadFolder.mkdir())
                throw new RuntimeException("Cannot create download folder (folder absolute path: "
                        + downloadFolder.getAbsolutePath() + ")");

    }

    public static void main(String[] args) {
        Dstore dstore = new Dstore(6666, 8989, 1000, "Dstore1");
        dstore.connectToController();
        dstore.startListening();
        // dstore.testMsg("FUCK From dstore");
    }
}

class DstoreReadThread extends Thread {
    Socket socket;

    public DstoreReadThread(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                System.out.println("DSTORE: " + line);
            }
            socket.close();
        } catch (Exception e) {
        }
    }// end run method
}// end ControllerThread class