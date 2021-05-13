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

    public void listen() throws IOException{
        for(;;){
            Socket socket = serverSocket.accept();
            ControllerInterface callback = positioningF.apply(socket);
            executor.submit(callback);
        }
    }

    public ControllerThread(int port, Function<Socket, ControllerInterface> positioningF) throws IOException{
        this.executor = Executors.newCachedThreadPool();
        this.serverSocket = new ServerSocket(port);
        this.positioningF = positioningF;

    }

    public void setPositioningF(Function<Socket, ControllerInterface> positioningF){
        this.positioningF = positioningF;
    }

    public ControllerThread(int port) throws IOException {
        this(port, null);
    }




}
