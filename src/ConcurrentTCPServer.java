import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class ConcurrentTCPServer {
    private ExecutorService service;
    private ServerSocket serverSocket;
    private Function<Socket, ControllerInterface> callbackSupplier;

    public interface ControllerInterface extends Runnable{
        void joinTokenHandler(JoinToken token, Socket socket, PrintWriter out);
        void storeTokenHandler(StoreToken token, Socket socket, PrintWriter out);
        void storeACKHanlder(StoreACKTOKEN token,Socket socket, PrintWriter out);
        void removeTokenHandler(RemoveToken token, Socket socket, PrintWriter out);
        void removeACKTokenHandler(RemoveACKToken token, Socket socket, PrintWriter out);
        void loadTokenHandler(LoadToken token, Socket socket, PrintWriter out);
        void listTokenHandler(ListToken token, Socket socket, PrintWriter out);
        void reloadTokenHandler(ReloadToken token,Socket socket, PrintWriter out);
        void rebalanceCompleteTokenHandler(RebalanceCompleteToken token,Socket socket, PrintWriter out);
    }

    public ConcurrentTCPServer(int port) throws IOException {
        this(port, null);
    }

    public ConcurrentTCPServer(int port, Function<Socket, ControllerInterface> callbackSupplier) throws IOException{
        this.service = Executors.newCachedThreadPool();
        this.serverSocket = new ServerSocket(port);
        this.callbackSupplier = callbackSupplier;

    }

    public void setCallbackSupplier(Function<Socket, ControllerInterface> callbackSupplier){
        this.callbackSupplier = callbackSupplier;
    }

    public void listen() throws IOException{
        for(;;){
            Socket socket = serverSocket.accept();
            ControllerInterface callback = callbackSupplier.apply(socket);
            service.submit(callback);
        }
    }


}
