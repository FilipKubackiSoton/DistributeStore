import java.io.PrintWriter;
import java.net.Socket;

public interface ControllerInterface extends Runnable{
    void joinTokenHandler(JoinToken token, Socket socket, PrintWriter out);
    void storeTokenHandler(StoreToken token, Socket socket, PrintWriter out);
    void storeACKHanlder(StoreACKTOKEN token, Socket socket, PrintWriter out);
    void removeTokenHandler(RemoveToken token, Socket socket, PrintWriter out);
    void removeACKTokenHandler(RemoveACKToken token, Socket socket, PrintWriter out);
    void loadTokenHandler(LoadToken token, Socket socket, PrintWriter out);
    void listTokenHandler(ListToken token, Socket socket, PrintWriter out);
    void reloadTokenHandler(ReloadToken token, Socket socket, PrintWriter out);
    void rebalanceCompleteTokenHandler(RebalanceCompleteToken token, Socket socket, PrintWriter out);
}