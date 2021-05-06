
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

class ControllerThread extends Thread {
    Socket socket;

    public ControllerThread(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            String line;
            Token token = null;
            while ((line = in.readLine()) != null) {
                ControllerLogger.getInstance().messageReceived(socket, line);
                token = Tokenizer.getToken(line);
                if (token instanceof JoinToken)
                    Controller.joinTokenHanlder((JoinToken) token);
                else if (token instanceof ListToken)
                    Controller.listTokenHandler((ListToken) token);
                else if (token instanceof StoreToken) {

                    System.out.println("STORE TOKE fuck hey " + ((StoreToken) token)._filename + " "
                            + ((StoreToken) token)._filesize);

                    new Thread() {
                        public synchronized void run() {
                            for (int i = 0; i < 5; i++) {
                                System.out.println("SOME" + i);
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                            }
                            return;
                        }
                    }.start();

                    /*
                     * new Thread(new FileTransferThread(socket, ((StoreToken) token)._filename,
                     * (int) ((StoreToken) token)._filesize)).start();
                     */
                    // out.println(Controller.getStoreToRequest());
                    out.println("STORE_TO 12334");

                    // out.println(Controller.getStoreToRequest());
                    // out.write(Controller.getStoreToRequest());
                    Controller.storeTokenHanlder((StoreToken) token);
                    ControllerLogger.getInstance().messageSent(socket, Controller.getStoreToRequest());
                } else {
                    System.out.println(line + " received");
                    out.println(line + " received");
                    out.write(line + " received");
                    ControllerLogger.getInstance().messageSent(socket, line + " received");
                    // tellClient(line + " received");
                }
            }
            socket.close();
        } catch (Exception e) {
        }
    }// end run method
}// end ControllerThread class

class FileTransferThread extends Thread {
    private Socket socket;
    private String filename;
    private int filesize;

    public FileTransferThread(Socket socket, String filename, int filesize) {
        this.socket = socket;
        this.filename = filename;
        this.filesize = filesize;

    }

    /*
    
    */
    @Override
    public void run() {
        System.out.println("STORE TOKE fuck hey2");
        for (;;) {
            try {
                byte[] mybytearray = new byte[(int) filesize];
                String filepath = "C" + filename; // Paths.get(System.getProperty("user.dir"), storename,
                                                  // filename).toString();
                System.out.println(filepath);
                InputStream is = socket.getInputStream();
                FileOutputStream fos = new FileOutputStream(filepath);
                BufferedOutputStream bos = new BufferedOutputStream(fos);
                int bytesRead = is.readNBytes(mybytearray, 0, (int) filesize);

                bos.write(mybytearray, 0, bytesRead);
                if (filesize == bytesRead) {
                    System.out.println("FILE IS VERYFIED");
                    bos.close();
                    is.close();
                    socket.close();
                    fos.close();
                    break;
                } else if (bytesRead == 0) {
                    System.out.println("WAITING...");
                    continue;
                } else {
                    bos.close();
                    is.close();
                    socket.close();
                    fos.close();
                    System.out.println("FILE not VERIFIED");
                }

            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                break;
            }
        }
    }

}