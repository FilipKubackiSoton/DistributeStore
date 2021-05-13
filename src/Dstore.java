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
                    else if (token instanceof RebalanceToken)
                        rebalanceTokenHandler((RebalanceToken) token, serverSocket, out);
                    else
                        System.out.println("MSGc " + msg);
                    System.out.println("ACKawaited: " + ACKawaited);
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

        public void executeRebalance(RebalanceToken token, Socket socket, PrintWriter out){
            for(String file : token._files_to_send.keySet()){
                List<Thread> threads = new ArrayList<>();
                for (int dstore : token._files_to_send.get(file)){
                    Thread thread = new Thread(() -> {
                        try {
                            Socket dstoreJ = new Socket();
                            socket.setSoTimeout(timeout);
                            dstoreJ.connect(new InetSocketAddress(InetAddress.getLocalHost(), dstore), timeout);

                            String request = Protocol.REBALANCE_STORE_TOKEN + " " + file + " " + files.get(file);

                            PrintWriter toD = new PrintWriter(dstoreJ.getOutputStream(), true);
                            BufferedReader fromD =new BufferedReader(new InputStreamReader(dstoreJ.getInputStream()));
                            File fileToD = new File(folder, file);
                            if(fileToD.exists()){
                                toD.println(request);
                                DstoreLogger.getInstance().messageSent(dstoreJ, request);
                                String msg = fromD.readLine();
                                DstoreLogger.getInstance().messageReceived(dstoreJ, msg);
                                if(Tokenizer.getToken(msg) instanceof ACKToken)
                                    dstoreJ.getOutputStream().write(Files.readAllBytes(fileToD.toPath()));
                            }
                        }catch(IOException e){e.printStackTrace(); }
                    });
                    threads.add(thread);
                    thread.start();
                    //System.out.println(dstore + " d2d communctiona ");
                }
                while(threads.stream().map(Thread::isAlive).collect(Collectors.toList()).contains(true))
                    Thread.onSpinWait();
            }
            //out.println(Protocol.REBALANCE_COMPLETE_TOKEN);
            //DstoreLogger.getInstance().messageSent(serverSocket, Protocol.REBALANCE_COMPLETE_TOKEN);

            for(String fileToRemove : token._files_to_remove){
                try {
                    Files.delete(new File(folder, fileToRemove).toPath());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                files.remove(fileToRemove);
            }

        }

        public void rebalanceTokenHandler(RebalanceToken token, Socket socket, PrintWriter out)  {
            // Removing files
            System.out.println("REBALANCE TOKEN HANDLER");

            ExecutorService rebalance = Executors.newSingleThreadExecutor();
            rebalance.submit(() -> {
                executeRebalance(token, socket, out);
            });


            try {
                rebalance.shutdown();
                if(rebalance.awaitTermination(timeout, TimeUnit.MILLISECONDS)){
                    out.println(Protocol.REBALANCE_COMPLETE_TOKEN);
                    DstoreLogger.getInstance().messageSent(socket, Protocol.REBALANCE_COMPLETE_TOKEN);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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
                    else if (token instanceof RebalanceStoreToken)
                        rebalanceStoreTokenHandler((RebalanceStoreToken)token, socket, out);
                    else
                        System.out.println("MSG" + msg);
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



        public void rebalanceStoreTokenHandler(RebalanceStoreToken token, Socket socket, PrintWriter out)  {
            try {
                System.out.println("REBALANCE STORE HANDLER FUCK Q@#$ " + token._filename + " - " + token._filesize);

                out.println(Protocol.ACK_TOKEN);
                DstoreLogger.getInstance().messageSent(socket, Protocol.ACK_TOKEN);


                OutputStream fileDestinatino = new FileOutputStream(new File(folder, token._filename));
                byte[] filePayload = socket.getInputStream().readNBytes((int) token._filesize);
                if(files.containsKey(token._filename)) return;

                System.out.println("PAYLOAD " + filePayload);
                files.put(token._filename, token._filesize);
                fileDestinatino.write(filePayload);
                fileDestinatino.close();

            }catch(IOException e){}

            //new PrintWriter(serverSocket.getOutputStream(), true).println(Protocol.STORE_ACK_TOKEN + " " + token._filename);
            //DstoreLogger.getInstance().messageSent(serverSocket,Protocol.STORE_ACK_TOKEN + " " + token._filename);

        }

        public void ackTokenHandler(ACKToken token, Socket socket, PrintWriter out){
            System.out.println("ACK TOKKEN HANDLER ACTIVE ");
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
