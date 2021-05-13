import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Controller extends ConcurrentTCPServer {
    private final int R;
    private int TIMEOUT;
    private int REBALANCE_PERIOD;

    private HashMap<Socket, ArrayList<String>> dStores = new HashMap<>();
    private final HashMap<Socket, Integer> dStoresPorts = new HashMap<>();

    private final HashMap<String, Integer> filesSaved = new HashMap<>();
    private final HashMap<String, Integer> filesUpd = new HashMap<>();
    private final HashMap<String, Map.Entry<Socket, List<Socket>>> ACKStore = new HashMap<>();
    private final HashMap<String, Map.Entry<Socket, List<Socket>>> ACKRemove = new HashMap<>();
    private final HashMap<Socket, ArrayList<String>> ACKRebalance = new HashMap<>();
    private final HashSet<Socket> alreadyRebalanced = new HashSet<>();

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
        //setCallbackSupplier(ControllerCallback::new);
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

        setCallbackSupplier(ControllerCallback::new);

    }

    private class ControllerCallback implements ControllerInterface {

        private Socket socket = null;
        private final HashMap<String, Integer> loadAttemptCounter = new HashMap<>(); // change it to attomic form if time


        public ControllerCallback(Socket socket) {
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
                    else if (token instanceof RebalanceToken) // REBALANCE
                        rebalanceTokenHandler((RebalanceToken)token, socket, out);
                    else if (token instanceof RebalanceCompleteToken) //REBALANCE COMPLETE
                        rebalanceCompleteTokenHandler((RebalanceCompleteToken) token, socket, out);
                    else
                        System.out.println(msg);

                    System.out.println("DSTORES: " + dStores);
                    System.out.println("ACKAWAITED: " + ACKStore);
                    System.out.println("ACKREMOVE: " + ACKRemove);
                    System.out.println("INPROGRESS " + filesUpd);
                    System.out.println("dstoresRebalanced " + alreadyRebalanced);
                    System.out.println("ACKawaitedRebalance " + ACKRebalance);

                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if(dStores.containsKey(socket)){
                    List<String> filesD = dStores.remove(socket);
                    dStoresPorts.remove(socket);
                    for(String file : filesD){
                        List<Socket> dstoresF = dStores.entrySet().stream().filter(x -> x.getValue().contains(file)).map(Map.Entry::getKey).collect(Collectors.toList());
                        if(dstoresF.isEmpty()){
                            filesSaved.remove(file);
                        }
                    }
                    new Thread(() ->{
                        rebalanceTokenHandler(null, null, null);
                    }).start();                }
            }
        }

        public void rebalanceCompleteTokenHandler(RebalanceCompleteToken token, Socket socket, PrintWriter out){
            if(!alreadyRebalanced.contains(socket) && rebalanceFlag.get()) alreadyRebalanced.add(socket);
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
            System.out.println("REMOVE ack");
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

            System.out.println("LIST TOKEN ACTIVE" + token._file_list + " ]");
            if(dStores.containsKey(socket)){
                listCounter.incrementAndGet();
                System.out.println("1");
                if(token._file_list.equals("")){
                    System.out.println("Store was empty");
                    dStores.put(socket, new ArrayList<>());
                }else{
                    System.out.println("Store was full");
                    for(String file : Tokenizer.getFileListFromRequest(token._file_list))
                        if(!dStores.get(socket).contains(file))
                            dStores.get(socket).add(file);
                }
            }else if(token._file_list.equals("")|| token._file_list.equals(" ") || token._file_list == null){
                while(rebalanceFlag.get())
                    Thread.onSpinWait();

                System.out.println("2");
                String request = Protocol.LIST_TOKEN + Tokenizer.parseWithBlankSpace(filesSaved.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList()));
                System.out.println("LIST " + request);
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
            System.out.println("RUNNING store ack handler");
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
    public void rebalanceTokenHandler(RebalanceToken token, Socket socket, PrintWriter out){
        if (dStores.size() < R) {
            rebalanceFlag.set(false);
            System.out.println("No need to rebalance. Not eneought stores."); return;

        }

        while(rebalanceFlag.get() || !filesUpd.isEmpty())
            Thread.onSpinWait();

        rebalanceFlag.set(true);

        if(!updateFiles()) {
            System.out.println("Updateing has failed");
            rebalanceFlag.set(false);
            return;
        }

        if(filesSaved.isEmpty()){
            System.out.println("No need to rebalance. Stores after update turned out to be empty. ");
            rebalanceFlag.set(false);
            return;
        }


        HashMap<Integer, List<String>> stores = new HashMap<>();
        HashMap<String , Integer> fileCounter = new HashMap<>();
        for(Socket socketU : dStores.keySet())
            stores.put(dStoresPorts.get(socketU), dStores.get(socketU));
        if(isBalanced(stores, filesSaved, (int) Math.ceil(R * filesSaved.size() /  dStores.size()) + 1)){
            System.out.println("STOREAS are already balanced. No need to rebalance them.");
            rebalanceFlag.set(false);
            return;
        }




        /*if(isBalanced(stores, files, ceil))
            System.out.println("STORES are already balanced no need to rebalance them");
            return;
        }
         */
        for(String filename : filesSaved.keySet())
            fileCounter.put(filename, 0);
        try {
            rebalance(stores, fileCounter);
        } catch (Exception e) {
            //e.printStackTrace();
        }finally {
            rebalanceFlag.set(false);
        }
    }
    public void rebalance(HashMap<Integer, List<String>> stores, HashMap<String, Integer> files){

        try{
            System.out.println("REBALANCE token handler active");
            int ceil =2 ;
            try {
                int N = dStores.size();
                if (N == 0 || N < R) return;
                int F = files.size();
                ceil = (int) Math.ceil(R * F / N) + 1;


                //            System.out.println(stores + " - " + files + " - " + ceil);
            }catch (Error e){
                System.out.println("ERROR 1");
            }

            List<String> allFiles = new ArrayList<>(files.keySet());
            List<Integer> allDstoresPorts = new ArrayList<>(stores.keySet());
            //          System.out.println("FUCK @2 " + allFiles + " - " + allDstoresPorts);
            HashMap<Integer, List<String>> rebalancedDstores = new HashMap<>();
            for(int port : stores.keySet())
                rebalancedDstores.put(port, new ArrayList<>());
//            System.out.println("FUCK @3 " + rebalancedDstores + " - " + files + " - " + ceil);
            rebalancedDstores = generateBalancedConfig(rebalancedDstores, files, ceil);

            //dstores = newDstores;
            System.out.println("dstore: " + dStores);
            System.out.println("Balanced dstore: " + rebalancedDstores);

            HashMap<Integer, List<String>> filesToAdd = new HashMap<>();
            HashMap<Integer, List<String>> filesToRemove = new HashMap<>();
            HashMap<Integer, List<String>> dstorePortsNew = new HashMap<>();
            for(Socket socket : dStores.keySet())
                dstorePortsNew.put(dStoresPorts.get(socket), dStores.get(socket));

            for(Integer dstore : allDstoresPorts){
                HashMap<Integer, List<String>> finalRebalancedDstores = rebalancedDstores;
                filesToRemove.put(dstore, dstorePortsNew.get(dstore).stream().filter(x -> !finalRebalancedDstores.get(dstore).contains(x)).collect(Collectors.toList()));
                filesToAdd.put(dstore, rebalancedDstores.get(dstore).stream().filter(x -> !dstorePortsNew.get(dstore).contains(x)).collect(Collectors.toList()));
            }

            HashMap<String, List<Integer>> sendFileTo = new HashMap<>();
            for(String file : allFiles)
                sendFileTo.put(file, filesToAdd.entrySet().stream().filter(x -> x.getValue().contains(file)).map(Map.Entry::getKey).collect(Collectors.toList()));

            System.out.println("FILESTOADD " + filesToAdd);
            System.out.println("FILESTOREMOVE " + filesToRemove);
            System.out.println("SENDFILETO " + sendFileTo);

            HashMap<String, Integer> filesFromToSend = new HashMap<>();

            for(Integer port : filesToAdd.keySet()){
                for(String file : filesToAdd.get(port)){
                    for(Socket socket : dStores.keySet()){
                        if(dStores.get(socket).contains(file)){
                            filesFromToSend.put(file, dStoresPorts.get(socket));
                            break;
                        }
                    }
                }
            }
            System.out.println("1 ");

            HashMap<String, Integer> addRequest = new HashMap<>();

            for(int port : filesToAdd.keySet())
                for(String file : filesToAdd.get(port))
                    for(Socket socket : dStores.keySet())
                        if(dStores.get(socket).contains(file) && filesToAdd.containsKey(dStoresPorts.get(socket)))
                            addRequest.put(file, dStoresPorts.get(socket));
            System.out.println("2");
            //System.out.println(addRequest);
            HashMap<Integer, HashMap<String, List<Integer>>> requests = new HashMap<>();
            for(int port : filesToAdd.keySet()){
                for(String fileToFetch : filesToAdd.get(port)){
                    requests.putIfAbsent(addRequest.get(fileToFetch), new HashMap<>());
                    requests.get(addRequest.get(fileToFetch)).putIfAbsent(fileToFetch, new ArrayList<>());
                    requests.get(addRequest.get(fileToFetch)).get(fileToFetch).add(port);
                }
            }
            System.out.println("3");
            System.out.println(requests);
            System.out.println(requests.keySet());
            System.out.println(requests.keySet().stream().filter(Objects::nonNull).collect(Collectors.toList()));

            HashMap<Integer, StringBuilder> requestToSend = new HashMap<>();
            for(int port : requests.keySet()){
                System.out.println("Port " + port);
                requestToSend.put(port, new StringBuilder(Protocol.REBALANCE_TOKEN));
                requestToSend.get(port).append(" ").append(requests.get(port).size());
                for(String file : requests.get(port).keySet()){
                    System.out.println("File" + file);
                    requestToSend.get(port).append(" ").append(file).append(" ").append(requests.get(port).get(file).size());
                    for(int targetPort : requests.get(port).get(file)){
                        System.out.println("Target " + targetPort);
                        requestToSend.get(port).append(" ").append(targetPort);
                    }

                }
            }
            System.out.println(requestToSend);
            System.out.println(requestToSend.keySet());
            if(requestToSend.keySet().size()>0) {

                // System.out.println("2");
                System.out.println("4");
                for (int port : filesToRemove.keySet()) {
                    if (!requestToSend.containsKey(port)) {
                        requestToSend.put(port, new StringBuilder(Protocol.REBALANCE_TOKEN + " 0"));
                    }
                    requestToSend.get(port).append(" ").append(filesToRemove.get(port).size());
                    if (filesToRemove.get(port).size() == 0) continue;
                    for (String fileToR : filesToRemove.get(port)) {
                        requestToSend.get(port).append(" ").append(fileToR);
                    }
                }
            }

            System.out.println("PARSED " + requestToSend);

            if(requestToSend.isEmpty()){
                String request = Protocol.REBALANCE_TOKEN + " 0 0";
                for(Socket socketU : dStores.keySet()){
                    new PrintWriter(socketU.getOutputStream(), true).println(request);
                    ControllerLogger.getInstance().messageSent(socketU , request);
                    //dstores.put(socketU, (ArrayList<String>) rebalancedDstores.get(dstoresPorts.get(socketU)));
                }
            }else{
                for(Socket socketU : dStores.keySet()){
                    new PrintWriter(socketU.getOutputStream(), true).println(requestToSend.get(dStoresPorts.get(socketU)));
                    ControllerLogger.getInstance().messageSent(socketU , requestToSend.get(dStoresPorts.get(socketU)).toString());
                    dStores.put(socketU, (ArrayList<String>) rebalancedDstores.get(dStoresPorts.get(socketU)));
                }
            }



            rebalancedDstores.clear();

            ExecutorService rebalanceACKawaited = Executors.newSingleThreadExecutor();
            HashMap<Integer, List<String>> finalRebalancedDstores1 = rebalancedDstores;
            rebalanceACKawaited.submit(()->{
                while(finalRebalancedDstores1.size() < requestToSend.size())
                    Thread.onSpinWait();
            });
            System.out.println("finalizing rebalance " + requestToSend);

            rebalanceACKawaited.shutdown();
            rebalanceACKawaited.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

            for(Socket socketU : alreadyRebalanced)
                dStores.put(socketU, (ArrayList<String>) rebalancedDstores.get(dStoresPorts.get(socketU)));
            //rebalanceRunning.set(false);
        }catch(IOException | InterruptedException e){

        }

    }

    public HashMap<Integer, List<String>> generateBalancedConfig(HashMap<Integer, List<String>> stores, HashMap<String, Integer> files, int ceil){
        //System.out.println("STORES " + stores);
        //if(stores.entrySet().stream().map(x -> x.getValue().size()).reduce(0, Integer::sum).equals(files.size()*R)) return stores;
        if(isBalanced(stores, files, ceil)) return stores;
        for(String fileU : files.keySet()){
            if(files.get(fileU)==R) continue;
            files.put(fileU, files.get(fileU)+1);

            for(int storeU : stores.keySet()){
                if(stores.get(storeU).contains(fileU)) continue;
                if(stores.get(storeU).size()==ceil) continue;
                stores.get(storeU).add(fileU);
                HashMap<Integer, List<String>> update = generateBalancedConfig(stores, files, ceil);
                if(update!=null)
                    return update;
                stores.get(storeU).remove(fileU);
            }

            files.put(fileU, files.get(fileU)-1);
        }
        return null;
    }

    public boolean isBalanced(HashMap<Integer, List<String>> stores, HashMap<String, Integer> files, int ceil){
        if(!stores.entrySet().stream().map(x -> x.getValue().size()).reduce(0, Integer::sum).equals(files.size()*R)) return false;
        HashMap<String, Integer> fileFrequency = new HashMap<>();
        for(String file : files.keySet())
            fileFrequency.put(file, 0);
        for(int store : stores.keySet()){
            for(String file : stores.get(store)){
                fileFrequency.put(file, fileFrequency.get(file)+1);
            }
        }
        for(String file : fileFrequency.keySet()){
            if(fileFrequency.get(file) != R) return false;
        }
        for(int store : stores.keySet()){
            if(!(stores.get(store).size()==ceil || stores.get(store).size()==(ceil-1))) return false;
        }
        return true;
    }

    public boolean updateFiles(){
        for(Socket socket : dStores.keySet()){
            try {
                new PrintWriter(socket.getOutputStream(), true).println(Protocol.LIST_TOKEN);
                ControllerLogger.getInstance().messageSent(socket , Protocol.LIST_TOKEN);

            } catch (IOException e) {
                return false;
            }
        }

        ExecutorService listW = Executors.newSingleThreadExecutor();
        listW.submit(() -> {
            for(;;){
                if(listCounter.get() < dStores.size()){
                    continue;
                }

                listCounter.set(0);
                break;
            }
        });
        listW.shutdown();
        try {
            return listW.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }

    }
}
