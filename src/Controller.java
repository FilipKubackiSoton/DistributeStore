import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Controller extends ControllerThread {
    private final int R;
    private int TIMEOUT;
    private int REBALANCE_PERIOD;

    private HashMap<Socket, ArrayList<String>> dStores = new HashMap<>();
    private final HashMap<Socket, Integer> dStoresPorts = new HashMap<>();

    private final HashMap<String, Integer> filesSaved = new HashMap<>();
    private final HashMap<String, Integer> filesUpd = new HashMap<>();
    private final HashMap<String, Map.Entry<Socket, List<Socket>>> ACKStore = new HashMap<>();
    private final HashMap<String, Map.Entry<Socket, List<Socket>>> ACKRemove = new HashMap<>();
    //private final HashMap<Socket, ArrayList<String>> ACKRebalance = new HashMap<>();
    //private final HashSet<Socket> alreadyRebalanced = new HashSet<>();

    private final AtomicInteger listCounter = new AtomicInteger(0);
    private final AtomicBoolean rebalanceFlag = new AtomicBoolean(false);

    public static void main(String[] args){
        try{
            if(args.length!=4)
                throw new IllegalArgumentException("Dstore requires 4 arguments");
            int port = Integer.parseInt(args[0]);
            int r = Integer.parseInt(args[1]);
            int timeout = Integer.parseInt(args[2]);
            int rebbal = Integer.parseInt(args[3]);


            try {
                Controller controller = new Controller(port, r, timeout, rebbal);
                controller.listen();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }catch(Exception e){}
    }


    public Controller(int port, final int R, int timeout, int rebalance_period) throws IOException {
        super(port);
        this.R = R;
        this.REBALANCE_PERIOD = rebalance_period;
        this.TIMEOUT = timeout;

        try {
            ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);
        } catch (IOException e) {
            e.printStackTrace();
        }
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("EXECUTING rebelance");
                //rebalanceTokenHandler(null, null, null);
            }
        }, REBALANCE_PERIOD, REBALANCE_PERIOD);

        setPositioningF(ControllerThreadImplementation::new);

    }

    private class ControllerThreadImplementation implements ControllerInterface {

        private Socket socket = null;
        private final HashMap<String, Integer> loadAttemptCounter = new HashMap<>(); // change it to attomic form if time


        public ControllerThreadImplementation(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            String msg;
            Token token;
            BufferedReader in;
            PrintWriter out;
            try {

                while (!socket.isClosed()) {

                    in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    out = new PrintWriter(socket.getOutputStream(), true);
                    msg = in.readLine();
                    token = Tokenizer.getToken(msg);
                    ControllerLogger.getInstance().messageReceived(socket, msg);

                    if (token instanceof JoinToken)
                        joinTokenHandler((JoinToken)token, socket, out);
                    else if (token instanceof StoreToken)   // STORE
                        storeTokenHandler((StoreToken) token, socket, out);
                    else if (token instanceof StoreACKTOKEN) // STORE ACK
                        storeACKHanlder(((StoreACKTOKEN)token), socket, out);
                    else if (token instanceof RemoveToken)  // REMOVE
                        removeTokenHandler((RemoveToken)token, socket, out);
                    else if (token instanceof RemoveACKToken) // REMOVE ACK
                        removeACKTokenHandler((RemoveACKToken)token, socket, out);
                    else if (token instanceof LoadToken)    // LOAD
                        loadTokenHandler((LoadToken)token, socket, out);
                    else if (token instanceof ListToken)    // LIST
                        listTokenHandler((ListToken)token, socket, out);
                    else if (token instanceof ReloadToken) // RELOAD
                        reloadTokenHandler((ReloadToken)token, socket, out);
                    //else if (token instanceof RebalanceToken) // REBALANCE
                      //  rebalanceTokenHandler((RebalanceToken)token, socket, out);
                    else if (token instanceof RebalanceCompleteToken) //REBALANCE COMPLETE
                        rebalanceCompleteTokenHandler((RebalanceCompleteToken) token, socket, out);
                    else
                        System.out.println(msg);

                   /* System.out.println("DSTORES: " + dStores);
                    System.out.println("ACKAWAITED: " + ACKStore);
                    System.out.println("ACKREMOVE: " + ACKRemove);
                    System.out.println("INPROGRESS " + filesUpd);
                    System.out.println("dstoresRebalanced " + alreadyRebalanced);
                    System.out.println("ACKawaitedRebalance " + ACKRebalance);

                    */

                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if(dStores.containsKey(socket)) {
                    List<String> filesD = dStores.remove(socket);
                    dStoresPorts.remove(socket);
                    for (String file : filesD) {
                        List<Socket> dstoresF = dStores.entrySet().stream().filter(x -> x.getValue().contains(file)).map(Map.Entry::getKey).collect(Collectors.toList());
                        if (dstoresF.isEmpty()) {
                            filesSaved.remove(file);
                        }
                    }
                    /*new Thread(() ->{
                        rebalanceTokenHandler(null, null, null);
                    }).start();

                     */
                }
            }
        }

        public void rebalanceCompleteTokenHandler(RebalanceCompleteToken token, Socket socket, PrintWriter out){
           // if(!alreadyRebalanced.contains(socket) && rebalanceFlag.get()) alreadyRebalanced.add(socket);
        }

        public void loadTokenHandler(LoadToken token, Socket socket, PrintWriter out){
            while(rebalanceFlag.get())
                Thread.onSpinWait();

            List<Socket> dstoresF = dStores.entrySet().stream().filter(x -> x.getValue().contains(token._filename)).map(Map.Entry::getKey).collect(Collectors.toList());

            if(!filesSaved.containsKey(token._filename) || dstoresF.isEmpty()){
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                //filesSaved.remove(token._filename);
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                ControllerLogger.getInstance().messageSent(socket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                return;
            }
            loadAttemptCounter.put(token._filename, 0);
            String request =Protocol.LOAD_FROM_TOKEN +" "+ dStoresPorts.get(dstoresF.remove(0)) + " " + filesSaved.get(token._filename);
            out.println(request);// send to first encounter store
            ControllerLogger.getInstance().messageSent(socket, request);

        }

        public void reloadTokenHandler(ReloadToken token, Socket socket, PrintWriter out){
            // add logger
            int attempt = loadAttemptCounter.getOrDefault(token._filename, 0)+1;
            loadAttemptCounter.put(token._filename, attempt);

            List<Socket> dstoresF = dStores.entrySet().stream().filter((e) -> e.getValue().contains(token._filename)).map(Map.Entry::getKey).collect(Collectors.toList());
            String request = (dstoresF.size() > attempt) ? Protocol.LOAD_FROM_TOKEN + " " + dStoresPorts.get(dstoresF.get(attempt)) : Protocol.ERROR_LOAD_TOKEN;
            out.println(request);
            ControllerLogger.getInstance().messageSent(socket, request);
        }

        public void removeACKTokenHandler(RemoveACKToken token, Socket socket, PrintWriter out) {
            //System.out.println("REMOVE ack");
            try{

                if(ACKRemove.get(token._filename) == null){
                    out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    ControllerLogger.getInstance().messageSent(socket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    return;
                }
                ACKRemove.get(token._filename).getValue().remove(socket);
                if(ACKRemove.get(token._filename).getValue().isEmpty()){

                    for(Socket socketU : dStores.keySet())
                        if(dStores.get(socketU).contains(token._filename))
                            dStores.get(socketU).remove(token._filename);



                    new PrintWriter(ACKRemove.get(token._filename).getKey().getOutputStream(), true).println(Protocol.REMOVE_COMPLETE_TOKEN);
                    ControllerLogger.getInstance().messageSent(ACKRemove.get(token._filename).getKey(), Protocol.REMOVE_COMPLETE_TOKEN);
                    //removeACKawaited.remove(token._filename);
                    filesUpd.remove(token._filename);
                    filesSaved.remove(token._filename);



                }else{
                    ACKRemove.get(token._filename).getValue().remove(socket);
                }
            }catch(IOException e){}

        }



        public void listTokenHandler(ListToken token, Socket socket, PrintWriter out){
            //while(rebalanceRunning.get())
            //  Thread.onSpinWait();

            //System.out.println("LIST TOKEN ACTIVE" + token._file_list + " ]");
            if(dStores.containsKey(socket)){
                listCounter.incrementAndGet();
                //System.out.println("1");
                if(token._file_list.equals("")){
                   // System.out.println("Store was empty");
                    dStores.put(socket, new ArrayList<>());
                }else{
                   // System.out.println("Store was full");
                    for(String file : Tokenizer.getFileListFromRequest(token._file_list))
                        if(!dStores.get(socket).contains(file))
                            dStores.get(socket).add(file);
                }
            }else if(token._file_list.equals("")|| token._file_list.equals(" ") || token._file_list == null){
                while(rebalanceFlag.get())
                    Thread.onSpinWait();

               // System.out.println("2");
                String request = Protocol.LIST_TOKEN + Tokenizer.parseWithBlankSpace(filesSaved.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList()));
                //System.out.println("LIST " + request);
                out.println(request);
                ControllerLogger.getInstance().messageSent(socket, request);
            }
        }



        public void joinTokenHandler(JoinToken token, Socket socket, PrintWriter out){

            while(rebalanceFlag.get())
                Thread.onSpinWait();
            //logger
            //File downloadFolder = new File("store"+STORE_COUNTER++);
            //if (!downloadFolder.exists())
            //  if (!downloadFolder.mkdir()) throw new RuntimeException("Cannot create download folder (folder absolute path: " + downloadFolder.getAbsolutePath() + ")");
            dStores.put(socket, new ArrayList<>());
            dStoresPorts.put(socket, token._port);
            //rebalanceTokenHanlder();
            ControllerLogger.getInstance().dstoreJoined(socket, token._port);

            /*new Thread(() ->{
                rebalanceTokenHandler(null, null, null);
            }).start();
             */



        }

        public void storeTokenHandler(StoreToken token, Socket socket, PrintWriter out) {

            while (rebalanceFlag.get()) {
                Thread.onSpinWait();
            }

            String filename = token._filename;
            int filesize = (int) token._filesize;

            if (dStores.size() < R) {
                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                ControllerLogger.getInstance().messageSent(socket, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                return;
            }else if (filesUpd.containsKey(filename) || filesSaved.containsKey(filename)) {
                out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                ControllerLogger.getInstance().messageSent(socket, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                return;
            }else
                filesUpd.put(filename, filesize);

            List<Socket> socketsDStores = new ArrayList<>(dStores.keySet());
            Collections.shuffle(socketsDStores);
            List<Socket> portsSocket = socketsDStores.stream().limit(R).collect(Collectors.toList());
            for(Socket socketP : portsSocket) dStores.get(socketP).add(filename);
            StringBuilder storeToRequest = new StringBuilder(Protocol.STORE_TO_TOKEN);
            for (int port : portsSocket.stream().map(dStoresPorts::get).collect(Collectors.toList()))
                storeToRequest.append(" ").append(port);

            out.println(storeToRequest);
            ControllerLogger.getInstance().messageSent(socket, storeToRequest.toString());

            ACKStore.put(filename, new AbstractMap.SimpleEntry<Socket, List<Socket>>(socket, portsSocket));

            new Timer().schedule(new TimerTask() {
                @Override
                public void run() {
                    //System.out.println("ACK AWITED: " + ACKawaited.get(filename).getValue());
                    //System.out.println("DSTORES KEY: " + dstores.keySet());
                    if (!Collections.disjoint(portsSocket, ACKStore.get(filename).getValue())){
                        filesUpd.remove(filename);
                        ACKStore.remove(filename);
                    }
                }
            }, TIMEOUT);
        }

        public void removeTokenHandler(RemoveToken token, Socket socket, PrintWriter out){
            try{
                if (dStores.size() < R) {
                    out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    ControllerLogger.getInstance().messageSent(socket, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    return;
                }

                String filename = token._filename;
                if(!filesSaved.containsKey(filename)){
                    out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    ControllerLogger.getInstance().messageSent(socket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    return;
                }

                List<Socket> socketsDStores = dStores.entrySet().stream().filter((e) -> e.getValue().contains(filename)).map(Map.Entry::getKey).collect(Collectors.toList()); //new ArrayList<>();
                /*for(Socket socketU: dstores.keySet())
                    if(dstores.get(socketU).contains(filename))
                        socketsDStores.add(socketU);

                 */
                filesUpd.put(filename, filesSaved.get(filename));

                ACKRemove.put(filename, new AbstractMap.SimpleEntry<Socket, List<Socket>>(socket, socketsDStores));

                new Timer().schedule(new TimerTask() {
                    @Override
                    public void run() {
                        if (!Collections.disjoint(dStores.keySet(), ACKRemove.get(filename).getValue())){
                            ACKRemove.remove(filename);
                            filesUpd.remove(filename);
                        }
                    }
                }, TIMEOUT);

                for(Socket store : socketsDStores){
                    dStores.get(store).remove(token._filename);
                    new PrintWriter(store.getOutputStream(), true).println(Protocol.REMOVE_TOKEN + " " + filename);
                    ControllerLogger.getInstance().messageSent(store, Protocol.REMOVE_TOKEN + " " + filename);
                    //dstores.get(store).remove(filename);
                }
            }catch(IOException e){}

        }

        public void storeACKHanlder(StoreACKTOKEN token, Socket socket, PrintWriter out){
           // System.out.println("RUNNING store ack handler");
            try{
                if(ACKStore.get(token._filename)==null){
                    out.print(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    ControllerLogger.getInstance().messageSent(socket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);

                }
                ACKStore.get(token._filename).getValue().remove(socket);

                if(ACKStore.get(token._filename).getValue().isEmpty()){
                    filesSaved.put(token._filename, filesUpd.remove(token._filename));
                    new PrintWriter(ACKStore.get(token._filename).getKey().getOutputStream(), true).println(Protocol.STORE_COMPLETE_TOKEN);
                    ControllerLogger.getInstance().messageSent(ACKStore.get(token._filename).getKey() , Protocol.STORE_COMPLETE_TOKEN);
                    //ACKStore.remove(token._filename);
                }
            }catch(IOException e){}
        }
    }

}
