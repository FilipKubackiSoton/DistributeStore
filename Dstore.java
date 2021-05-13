import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Dstore extends ControllerThread {
    private int timeout;

    private final Socket serverSocket = new Socket();
    private final HashMap<String, Integer> files = new HashMap<>();
    private final HashMap<Socket, String> ACKawaited = new HashMap<>();
    private File folder;

    public Dstore(int port, int cport, int timeout, File folder) throws IOException {
        super(port);
        setPositioningF(DstoreCallback::new);
        DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, port);
        this.timeout = timeout;
        this.folder = folder;

        serverSocket.connect(new InetSocketAddress(InetAddress.getLocalHost(), cport), timeout);
        new Thread(new DStoreConnection()).start();
        new PrintWriter(serverSocket.getOutputStream(), true).println(Protocol.JOIN_TOKEN + " " + port);
        DstoreLogger.getInstance().messageSent(serverSocket, Protocol.JOIN_TOKEN + " " + port);

    }

    private class DStoreConnection implements Runnable{

        @Override
        public void run(){
            BufferedReader in;
            PrintWriter out;
            String msg;
            Token token;
            try{
                while(!serverSocket.isClosed()){
                    in = new BufferedReader(new InputStreamReader(serverSocket.getInputStream()));
                    out = new PrintWriter(serverSocket.getOutputStream(), true);

                    msg = in.readLine();
                    token = Tokenizer.getToken(msg);
                    DstoreLogger.getInstance().messageReceived(serverSocket, msg);

                    if (token instanceof ListToken)
                        listTokenHandler((ListToken) token, serverSocket, out);
                    else if(token instanceof RemoveToken)
                        removeTokenHandler((RemoveToken)token, serverSocket, out);
                    else
                        System.out.println(msg);
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }

        public void listTokenHandler(ListToken token, Socket socket, PrintWriter out){
            if(!token._file_list.equals("")) return;
            String request =Protocol.LIST_TOKEN + Tokenizer.parseWithBlankSpace(files.keySet());
            out.println(request);
            DstoreLogger.getInstance().messageSent(socket,request);
        }


        public void removeTokenHandler(RemoveToken token, Socket socket, PrintWriter out){

            File file = new File(folder, token._filename);
            if(!files.containsKey(token._filename) || !file.exists()){
                if(!file.exists())files.remove(token._filename);
                String request =Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + token._filename;
                out.println(request);
                DstoreLogger.getInstance().messageSent(socket, request);
                return;
            }

            try {
                files.remove(token._filename);
                Files.delete(file.toPath());
                String request =Protocol.REMOVE_ACK_TOKEN + " " + token._filename;
                out.println(request);
                DstoreLogger.getInstance().messageSent(socket, request);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    private class DstoreCallback implements ControllerInterface {

        private Socket socket = null;

        public DstoreCallback(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run(){
            BufferedReader in;
            PrintWriter out;
            String msg;
            Token token;

            try {
                while (!serverSocket.isClosed() && !socket.isClosed()) {
                    socket.setSoTimeout(timeout);
                    in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    out = new PrintWriter(socket.getOutputStream(), true);

                    msg = in.readLine();
                    token = Tokenizer.getToken(msg);
                    DstoreLogger.getInstance().messageReceived(socket, msg);
                    if(token instanceof StoreToken)
                        storeTokenHanlder((StoreToken) token, socket, out);
                    else if (token instanceof LoadDataToken)
                        loadDataTokenHandler((LoadDataToken)token, socket, out);
                    else if (token instanceof ACKToken)
                        ackTokenHandler((ACKToken) token, socket, out);
                    else
                        System.out.println(msg);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                if(serverSocket.isClosed()) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public void ackTokenHandler(ACKToken token, Socket socket, PrintWriter out){
            //System.out.println("ACK TOKKEN HANDLER ACTIVE ");
            try {
                if(ACKawaited.containsKey(socket))
                    socket.getOutputStream().write(Files.readAllBytes(new File(folder, ACKawaited.get(socket)).toPath()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void loadDataTokenHandler(LoadDataToken token, Socket socket, PrintWriter out) {
            try{
                File file = new File(folder, token._filename);

                if(!file.exists()){
                    files.remove(token._filename);
                    socket.close();
                }else if (!files.containsKey(token._filename))
                    socket.close();
                else
                    socket.getOutputStream().write(Files.readAllBytes(file.toPath()));

            }catch(IOException e){
                e.printStackTrace();
            }

        }

        public void storeTokenHanlder(StoreToken token, Socket socket, PrintWriter out) {
            try{
                files.put(token._filename, (int) token._filesize);
                out.println(Protocol.ACK_TOKEN);
                DstoreLogger.getInstance().messageSent(socket,Protocol.ACK_TOKEN);

                byte[] filePayload = socket.getInputStream().readNBytes((int)token._filesize);
                OutputStream fileDestinatino = new FileOutputStream(new File(folder, token._filename));
                fileDestinatino.write(filePayload);
                fileDestinatino.close();

                String request = Protocol.STORE_ACK_TOKEN + " " + token._filename;
                new PrintWriter(serverSocket.getOutputStream(), true).println(request);
                DstoreLogger.getInstance().messageSent(serverSocket,request);
            }catch(IOException e){
                e.printStackTrace();
            }

        }

        @Override
        public void joinTokenHandler(JoinToken token, Socket socket, PrintWriter out) {

        }

        @Override
        public void storeTokenHandler(StoreToken token, Socket socket, PrintWriter out) {

        }

        @Override
        public void storeACKHanlder(StoreACKTOKEN token, Socket socket, PrintWriter out) {

        }

        @Override
        public void removeTokenHandler(RemoveToken token, Socket socket, PrintWriter out) {

        }

        @Override
        public void removeACKTokenHandler(RemoveACKToken token, Socket socket, PrintWriter out) {

        }

        @Override
        public void loadTokenHandler(LoadToken token, Socket socket, PrintWriter out) {

        }

        @Override
        public void listTokenHandler(ListToken token, Socket socket, PrintWriter out) {

        }

        @Override
        public void reloadTokenHandler(ReloadToken token, Socket socket, PrintWriter out) {

        }

        @Override
        public void rebalanceCompleteTokenHandler(RebalanceCompleteToken token, Socket socket, PrintWriter out) {

        }
    }


    public static void main(String[] args) {
        try{
            if(args.length!=4)
                throw new IllegalArgumentException("Dstore requires 4 arguments");
            int port = Integer.parseInt(args[0]);
            int cport = Integer.parseInt(args[1]);
            int timeout = Integer.parseInt(args[2]);
            File folder = new File(args[3]);
            if (!folder.exists()) {
                folder.mkdir();
            } else if (!folder.isDirectory()) {
                System.err.println("Invalid target directory");
                System.exit(1);
            }

            try {
                Dstore store = new Dstore(port, cport, timeout, folder);
                store.listen();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }catch(Exception e){}
    }
}
