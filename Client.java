
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Client {

    private int cport;
    private int timeout;
    private Logger.LoggingType logger;
    private String name;
    private Socket socket;

    public Client(int cport, int timeout, Logger.LoggingType logger) {
        this.cport = cport;
        this.timeout = timeout;
        this.logger = logger;
        this.name = "client" + System.currentTimeMillis();
        try {
            this.socket = new Socket("localhost", cport);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private class ClientReadThread extends Thread {
        Socket socket;

        public ClientReadThread(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            System.out.println("Start read thread");
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String line;
                while (true) {
                    line = in.readLine();
                    System.out.println(line + " response");
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
            }
        }// end run method
    }// end ClientReadThread

    private class ClientWriteThread extends Thread {
        Socket socket;

        public ClientWriteThread(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            // testMsg("From client ");
            // store(new File("tmp1.txt"));

            // testMsg("From client ");
            store(new File("tmp1.txt"));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            // tellDstore(6666, "FROM CLIENT");

            // store(new File("tmp1.txt"));
            // store(new File("tmp1.txt"));

        }// end run method
    }// end ClientReadThread

    public void startListening() {
        try {
            Socket ss = new Socket("localhost", this.cport);
            new Thread(new ClientWriteThread(ss)).start();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public synchronized void store(File file) {
        try {
            Token token;
            String response;
            String filename = file.getName();
            long filesize = Files.size(Paths.get(file.getPath()));
            BufferedReader inC = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            tellController(Protocol.STORE_TOKEN + " " + filename + " " + filesize);
            Thread.sleep(100);
            response = inC.readLine();
            System.out.println(response);
            token = Tokenizer.getToken(response);
            if (token instanceof StoreToToken)
                storeToHandler((StoreToToken) token);

            // sendFile(file, filesize);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public synchronized void storeToHandler(StoreToToken token) {
        System.out.println("HANDELING STORE_TO_TOKEN");
    }

    public synchronized void sendFile(File file, long filesize) {
        try {
            FileInputStream fos = new FileInputStream(file);
            Socket socket = new Socket("localhost", cport);
            BufferedOutputStream bos = new BufferedOutputStream(socket.getOutputStream());
            byte[] buf = new byte[(int) filesize];
            int bytesRead = fos.readNBytes(buf, 0, (int) filesize);
            bos.write(buf, 0, bytesRead);
            bos.close();
            if ((int) filesize == bytesRead)
                System.out.println("FILE IS VERYFIED");
            else
                System.out.println("FILE not VERIFIED");

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void testMsg(String msg) {
        try {
            for (int i = 0; i < 2; i++) {
                Socket socket = new Socket("localhost", this.cport);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                out.println(msg + i);
                System.out.println(msg + i);
                Thread.sleep(1000);
                System.out.println(in.readLine());
                // out.close();
                // in.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void tellDstore(int port, String msg) {
        try {
            Socket socket = new Socket("localhost", port);
            tell(socket, msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void tellController(String msg) {

        // Socket socket = new Socket("localhost", this.cport);
        tell(socket, msg);

    }

    public void tell(Socket socket, String msg) {
        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Client client = new Client(8989, 1000, Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.startListening();
        /*
         * new Thread() { public void run() { // client.store(new File("tmp1.txt")); //
         * client.testMsg("FUCK Client "); } }.start();
         */

    }

}
