import java.util.*;
import java.util.stream.Collectors;

class Tokenizer {
    Tokenizer() {
        ;
    }

    public static Token getToken(String req) {

        StringTokenizer sTokenizer = new StringTokenizer(req);
        if (!(sTokenizer.hasMoreTokens())) {
            return null;
        }
        String firstToken = sTokenizer.nextToken();

        /**
         * Tokens from Clients
         */
        try {
            if (firstToken.equals(Protocol.STORE_TOKEN)) {
                return sTokenizer.hasMoreTokens() ? new StoreToken(req, sTokenizer.nextToken(), Long.parseLong(sTokenizer.nextToken())) : null;
            }

            if (firstToken.equals(Protocol.LIST_TOKEN)) {
                return !sTokenizer.hasMoreTokens() ? new ListToken(req, "") : new ListToken(req, parseWithBlankSpace(sTokenizer));
            }

            if (firstToken.equals(Protocol.LOAD_TOKEN)) {
                return sTokenizer.hasMoreTokens() ? new LoadToken(req, sTokenizer.nextToken()) : null;
            }

            if (firstToken.equals(Protocol.LOAD_DATA_TOKEN)) {
                return sTokenizer.hasMoreTokens() ? new LoadDataToken(req, sTokenizer.nextToken()) : null;
            }

            if (firstToken.equals(Protocol.RELOAD_TOKEN)) {
                return sTokenizer.hasMoreTokens() ? new ReloadToken(req, sTokenizer.nextToken()) : null;
            }

            if (firstToken.equals(Protocol.REMOVE_TOKEN)) {
                return sTokenizer.hasMoreTokens() ? new RemoveToken(req, sTokenizer.nextToken()) : null;
            }

            /**
             * Tokens from Controller
             */
            if (firstToken.equals(Protocol.STORE_TO_TOKEN)) {
                return (sTokenizer.hasMoreTokens()) ? new StoreToToken(req, parseWithBlankSpace(sTokenizer)) : null;
            }

            if (firstToken.equals(Protocol.STORE_COMPLETE_TOKEN)) {
                return sTokenizer.hasMoreTokens() ? new StoreCompleteToken(req) : null;
            }

            if (firstToken.equals(Protocol.LOAD_FROM_TOKEN)) {
                return (sTokenizer.hasMoreTokens()) ? new LoadFromToken(req, Integer.parseInt(sTokenizer.nextToken()), Integer.parseInt(sTokenizer.nextToken())) : null;
            }

            if (firstToken.equals(Protocol.REMOVE_COMPLETE_TOKEN)) {
                return (!sTokenizer.hasMoreTokens()) ? new RemoverCompleteToken(req) : null;
            }

            if (firstToken.equals(Protocol.REBALANCE_TOKEN)) {
                return (sTokenizer.hasMoreTokens()) ? parseRebalanceRequest(req, sTokenizer) : null;
            }


            /**
             * Tokens from Dstores
             */

            if (firstToken.equals(Protocol.ACK_TOKEN)) {
                return (!sTokenizer.hasMoreTokens()) ? new ACKToken(req) : null;
            }

            if (firstToken.equals(Protocol.JOIN_TOKEN)) {
                return sTokenizer.hasMoreTokens() ? new JoinToken(req, Integer.parseInt(sTokenizer.nextToken())) : null;
            }

            if (firstToken.equals(Protocol.STORE_ACK_TOKEN)) {
                return sTokenizer.hasMoreTokens() ? new StoreACKTOKEN(req, sTokenizer.nextToken()) : null;
            }

            if (firstToken.equals(Protocol.REMOVE_ACK_TOKEN)) {
                return (sTokenizer.hasMoreTokens()) ? new RemoveACKToken(req, sTokenizer.nextToken()) : null;
            }

            if (firstToken.equals(Protocol.REBALANCE_STORE_TOKEN)) {
                return (sTokenizer.hasMoreTokens()) ? new RebalanceStoreToken(req, sTokenizer.nextToken(), Integer.parseInt(sTokenizer.nextToken())) : null;
            }

            if (firstToken.equals(Protocol.REBALANCE_COMPLETE_TOKEN)) {
                return (!sTokenizer.hasMoreTokens()) ? new RebalanceCompleteToken(req) : null;
            }
        }catch(Exception e){
            return null;
        }
        return null;
    }

    public static RebalanceToken parseRebalanceRequest(String req, StringTokenizer sTokenizer){
        StringBuilder sb = new StringBuilder();

        try{
            int numFTS = Integer.parseInt(sTokenizer.nextToken());
            HashMap<String, List<Integer>> filesToSend = new HashMap<>();
            List<String> filesToRemove = new ArrayList<>();

            for(int i = 0; i< numFTS; i++){
                String file = sTokenizer.nextToken();
                filesToSend.put(file, new ArrayList<>());
                int dstoreNum = Integer.parseInt(sTokenizer.nextToken());
                for(int j = 0; j< dstoreNum; j++)
                    filesToSend.get(file).add(Integer.parseInt(sTokenizer.nextToken()));
            }
            int numFTR = Integer.parseInt(sTokenizer.nextToken());
            for(int i =0; i< numFTR; i++)
                filesToRemove.add(sTokenizer.nextToken());

            return new RebalanceToken(req, filesToSend, filesToRemove);
        }catch (NumberFormatException nfe){
            return null;
        }
    }


    public static List<String> getFileListFromRequest(String fileList){


        StringTokenizer sTokenizer = new StringTokenizer(fileList.trim());
        List<String> res = new ArrayList<>();
        while(sTokenizer.hasMoreTokens()){
            res.add(sTokenizer.nextToken());
        }
        return res;
    }

    public static List<Integer> getPortListStoreTo(String storeToRequest){
        return Arrays.stream(storeToRequest.split(" "))
                .skip(1).map(Integer::parseInt).collect(Collectors.toList());
    }

    public static String parseWithBlankSpace(StringTokenizer sTokenizer) {
        ArrayList<Integer> portList = new ArrayList<>();
        StringBuilder sb = new StringBuilder("");
        while (sTokenizer.hasMoreTokens())
            sb.append(" ").append(sTokenizer.nextToken());
        return sb.toString();
    }

    public static String parseWithBlankSpace(Collection<String> ... toPars){
        StringBuilder sb = new StringBuilder();
        for(Collection<String> arg : toPars)
            sb.append(parseWithBlankSpace(arg));
        return sb.toString();
    }

    public static String parseWithBlankSpace(Collection<String> toPars){
        StringBuilder sb = new StringBuilder("");
        for(String arg : toPars)
            sb.append(" ").append(arg);
        return sb.toString();
    }
}


/**
 * The Token Prototype.
 */
abstract class Token {
    String _req;
}

/**
 * Tokens from Clients
 */

class ListToken extends Token {
    String _file_list;
    ListToken(String req) {
        this._req = req;
        this._file_list = null;
    }

    ListToken(String req, String file_list){
        this._req = req;
        this._file_list = file_list;
    }
}

class StoreToken extends Token {
    String _filename;
    long _filesize;

    StoreToken(String req, String filename, long filesize) {
        this._req = req;
        this._filename = filename;
        this._filesize = filesize;
    }
}

class LoadToken extends Token{
    String _filename;

    public LoadToken(String req, String _filename) {
        this._req = req;
        this._filename = _filename;
    }
}

class LoadDataToken extends Token{
    String _filename;

    public LoadDataToken(String req, String _filename) {
        this._req = req;
        this._filename = _filename;
    }
}

class ReloadToken extends Token{
    String _filename;

    public ReloadToken(String req, String _filename) {
        this._req = req;
        this._filename = _filename;
    }
}

class RemoveToken extends Token{
    String _filename;

    public RemoveToken(String req, String _filename) {
        this._req = req;
        this._filename = _filename;
    }
}

/**
 * Tokens from Controller
 */

class StoreToToken extends Token {
    String _portList;

    StoreToToken(String req, String portList) {
        this._req = req;
        _portList = portList;
    }
}

class StoreCompleteToken extends Token {

    StoreCompleteToken(String req) {
        this._req = req;
    }
}

class LoadFromToken extends Token{
    int port;
    int filesize;

    public LoadFromToken(String req, int port, int filesize) {
        this._req = req;
        this.port = port;
        this.filesize = filesize;
    }
}

class RemoverCompleteToken extends Token{
    public RemoverCompleteToken(String req) {
        this._req = req;
    }
}

class RebalanceToken extends Token{
    HashMap<String, List<Integer>> _files_to_send;
    List<String> _files_to_remove;

    public RebalanceToken(String req, HashMap<String, List<Integer>> _files_to_send,  List<String> _files_to_remove) {
        this._req = req;
        this._files_to_send = _files_to_send;
        this._files_to_remove = _files_to_remove;
    }
}

/**
 * Tokens from Dstores
 */

class JoinToken extends Token {
    int _port;

    JoinToken(String req, int port) {
        this._req = req;
        this._port = port;
    }
}

class ACKToken extends Token {
    ACKToken(String req) {
        this._req = req;
    }
}

class StoreACKTOKEN extends Token{
    String _filename;
    StoreACKTOKEN(String req, String filename){
        this._req = req;
        this._filename = filename;
    }
}

class RemoveACKToken extends Token{
    String _filename;
    RemoveACKToken(String req, String filename){
        this._req = req;
        this._filename = filename;
    }
}

class RebalanceStoreToken extends Token{
    String _filename;
    int _filesize;

    public RebalanceStoreToken(String req, String _filename, int _filesize) {
        this._req = req;
        this._filename = _filename;
        this._filesize = _filesize;
    }
}

class RebalanceCompleteToken extends Token{
    public RebalanceCompleteToken(String req) {
        this._req = req;
    }
}
