package com.company;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public class Server {

    public static Map<String, PrintWriter> _clientMap = Collections.synchronizedMap(new HashMap<String,PrintWriter>(50));
    public static Map<String, PrintWriter> _storeWriterMap = Collections.synchronizedMap(new HashMap<String, PrintWriter>(50));
    public static Map<String, Integer> _storePortMap = Collections.synchronizedMap(new HashMap<String, Integer>(50));
    public static int stores_counter = 0;
    public static int client_counter = 0;
    public static int R = 3;
    public static boolean readyToStore = false;
    public static boolean storeInProgress = false;
    public static Stack<Token> pendingRequests = new Stack<>();

    /**
     * Wait for a connection request.
     * @param port
     * @throws IOException
     */
    void startListening(int port) throws IOException
    {
        System.out.println("Server is listening on port: " + port);
        ServerSocket listener = new ServerSocket(port);
        while(true){
            Socket client = listener.accept();
            new ServerThread(client, this).start();
        }
    }

    public static void main(String[] args) throws  IOException
    {
        if(args.length!=1){
            System.out.println("Usage: java ChatServer <port>");
            return;
        }

        new Server().startListening(Integer.parseInt(args[0]));
    }

    synchronized static void tell(String sender, String rcpt, String msg){
        String txt = sender + ":" + msg;
        PrintWriter pw = _clientMap.get(rcpt);
        if(pw==null){// No client with the specified name
            System.out.println("spierdolilo sie");
            return;
        }
        pw.println(txt);
        pw.flush();
    }

    public static void add_client(String name, PrintWriter writer){
        _clientMap.put(name, writer);
        client_counter++;
    }

    public static void remove_client(String name){
        _clientMap.remove(name);
        client_counter--;
    }

    public static void add_store(String name, int port, PrintWriter writer){
        _storeWriterMap.put(name, writer);
        _storePortMap.put(name, port);
        stores_counter++;
        readyToStore = stores_counter>=R;

    }

    public static void remove_store(String name){
        _storeWriterMap.remove(name);
        _storePortMap.remove(name);
        stores_counter--;
        readyToStore = stores_counter>=R;
    }

}
