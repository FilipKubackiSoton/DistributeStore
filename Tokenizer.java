import java.util.ArrayList;
import java.util.StringTokenizer;

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

        if (firstToken.equals(Protocol.STORE_TOKEN)) {
            return sTokenizer.hasMoreTokens()
                    ? new StoreToken(req, sTokenizer.nextToken(), Long.parseLong(sTokenizer.nextToken()))
                    : null;
        }

        /**
         * Tokens from Controller
         */
        if (firstToken.equals(Protocol.STORE_TO_TOKEN)) {
            if (sTokenizer.hasMoreTokens()) {
                ArrayList<Integer> portList = new ArrayList<>();
                while (sTokenizer.hasMoreTokens()) {
                    portList.add(Integer.parseInt(sTokenizer.nextToken()));
                }
                return new StoreToToken(req, portList);
            } else
                return null;
        }

        if (firstToken.equals(Protocol.STORE_COMPLETE_TOKEN)) {
            return sTokenizer.hasMoreTokens() ? new StoreCompleteToken(req) : null;
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

        ///////////////////////////////////////////////////////

        if (firstToken.equals("EXIT")) {
            return new ExitToken(req);
        }

        if (firstToken.equals("JOIN_CLIENT")) {
            return sTokenizer.hasMoreTokens() ? new JoinClientToken(req, sTokenizer.nextToken()) : null;
        }

        if (firstToken.equals("JOIN_STORE")) {
            return sTokenizer.hasMoreTokens()
                    ? new JoinStoreToken(req, sTokenizer.nextToken(), Integer.parseInt(sTokenizer.nextToken()))
                    : null;
        }

        if (firstToken.equals("FILE_SAVE")) {
            if (!sTokenizer.hasMoreTokens())
                return null;
            ArrayList<Byte> fileContent = new ArrayList<>();
            while (sTokenizer.hasMoreTokens())
                fileContent.add(Byte.parseByte(sTokenizer.nextToken()));
            return new FileSaveToken(req, fileContent);

        }

        return null;
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
    ListToken(String req) {
        this._req = req;
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

/**
 * Tokens from Controller
 */

class StoreToToken extends Token {
    ArrayList<Integer> _portList;

    StoreToToken(String req, ArrayList<Integer> portList) {
        this._req = req;
        _portList = portList;
    }
}

class StoreCompleteToken extends Token {

    StoreCompleteToken(String req) {
        this._req = req;
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

///////////////////////////////////////

class FileSaveToken extends Token {
    ArrayList<Byte> _payload;

    FileSaveToken(String req, ArrayList<Byte> payload) {
        this._req = req;
        this._payload = payload;
    }
}

class JoinClientToken extends Token {
    String _name;

    JoinClientToken(String req, String name) {
        this._req = req;
        this._name = name;
    }
}

class JoinStoreToken extends Token {
    String _name;
    int _port;

    JoinStoreToken(String req, String name, int port) {
        this._req = req;
        this._name = name;
        this._port = port;
    }
}

class ExitToken extends Token {
    ExitToken(String req) {
        this._req = req;
    }
}

class TellToken extends Token {
    String _rcpt, _msg;

    TellToken(String req, String rcpt, String msg) {
        this._req = req;
        this._rcpt = rcpt;
        this._msg = msg;
    }
}
