import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class ControllerThread {

    private ServerSocket serverSocket;
    private Function<Socket, ControllerInterface> positioningF;
    private ExecutorService executor;

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

    public void listen() throws IOException{
        for(;;){
            Socket socket = serverSocket.accept();
            ControllerInterface callback = positioningF.apply(socket);
            executor.submit(callback);
        }
    }

    public ControllerThread(int port, Function<Socket, ControllerInterface> callbackSupplier) throws IOException{
        this.executor = Executors.newCachedThreadPool();
        this.serverSocket = new ServerSocket(port);
        this.positioningF = callbackSupplier;

    }

    public void setPositioningF(Function<Socket, ControllerInterface> positioningF){
        this.positioningF = positioningF;
    }

    public ControllerThread(int port) throws IOException {
        this(port, null);
    }




}
