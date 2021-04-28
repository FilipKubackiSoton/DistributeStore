package com.company;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ServerThread extends Thread{
    private Socket _socket;
    private final Server server;
    private String _name = null;
    private BufferedReader _socketIn;
    private PrintWriter _socketOut;
    private boolean _isClient = false;
    private boolean _isStore = false;

    ServerThread(Socket socket, Server server) throws IOException
    {
        _socket = socket;
        this.server = server;
        //_clientSocket.setSoTimeout(timeout);
        _socketIn = new BufferedReader( new InputStreamReader( socket.getInputStream() ) );
        _socketOut = new PrintWriter( new OutputStreamWriter( socket.getOutputStream()), true);
    }

    public void run()
    {

            String message="";// =_socketIn.readLine();
            Token token;//= Tokenizer.getToken(message);
            System.out.println("First message: " + message);

            /**
             * Add R stores before handeling any request form client
             */
            synchronized (this){
                while(Server.stores_counter < Server.R){
                    try {
                        message = _socketIn.readLine();
                        System.out.println("Waiting for stores " + Server.stores_counter+ " " + message);
                        token = Tokenizer.getToken(message);
                        if(token instanceof JoinStoreToken) addStore(token); // add store
                        else if(token instanceof JoinClientToken) addClient(token); // add client
                        else Server.pendingRequests.add(token); // pend token to handle it later on
                        this.wait(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }catch (IOException e){
                        System.out.println("IOException whtile adding first R stores");
                        e.printStackTrace();
                    }catch (IllegalMonitorStateException e){
                        System.out.println("STh with motnitor happends");
                        e.printStackTrace();
                    }
                }
            }
            System.out.println("Pending requests: " + Server.pendingRequests);
            /*synchronized (this){
                while(!Server.pendingRequests.isEmpty()) {
                    try {
                        token = Server.pendingRequests.pop();
                        if (token instanceof StoreToken) storeFile(token);
                        else System.out.println(token._req);
                        this.wait(10);
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                    }
                }
            }
             */


        try {
            token = Tokenizer.getToken(_socketIn.readLine());
        } catch (IOException e) {
            e.printStackTrace();
        }



            System.out.println("Client map: " + Server._clientMap.size() + " " + Server._clientMap);
            //System.out.println("Store writer map: " + Server._storeWriterMap.size() + " " + Server._storeWriterMap);
            System.out.println("Store ports map: " + Server._storePortMap.size() + " " + Server._storePortMap);
            token = Tokenizer.getToken(message);

            /**
             * Start listening to the user/stores messages
             */
            while(!(token instanceof ExitToken)){

                try {

                    message = _socketIn.readLine();
                    token = Tokenizer.getToken(message);
                    if(token instanceof JoinStoreToken){
                        addStore(token);
                        continue;
                    }
                    if(token instanceof JoinClientToken){
                        addClient(token);
                        continue;
                    }

                    /**
                     * Part of a thread to handle tokens from client
                     */
                    if(_isClient){
                     if(token instanceof StoreToken){
                         storeFile(token);
                     }else{
                         System.out.println(message);
                     }
                    }

                    /**
                     * Part of a thread to handle tokens from store
                     */
                    if(_isStore){
                        System.out.println(message);
                    }
                    token = Tokenizer.getToken(_socketIn.readLine());
                } catch (IOException ioException) {
                    System.out.println(message);
                    ioException.printStackTrace();
                }

            }
        try {
            _socket.close();
        } catch (IOException ioException) {
            System.out.println("Counght I/O Exception. Check token message");
            ioException.printStackTrace();
        }finally {
            if(_isClient)
                Server.remove_client(_name);
            if(_isStore)
                removeStore();
            System.out.println("Client map: " + Server._clientMap.size() + " " + Server._clientMap);
        }
    }

    public void storeFile(Token token){
        try {
            Server.storeInProgress = true;
            while(!Server.readyToStore){;}
            System.out.println("Store request: " + ((StoreToken) token)._filename + " - " + ((StoreToken) token)._filesize);
            long fileSize = Files.size(Paths.get(System.getProperty("user.dir"), ((StoreToken) token)._filename));
            Server.tell("SERVER", _name, "STORE " + ((StoreToken) token)._filename + " " + fileSize);
            String msg = "STORE_TO";
            for (int port : Server._storePortMap.values())
                msg += " " + port;

            Server.tell("SERVER", _name, msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addRStores(){
        synchronized (this){
            int counter = 0;
            try {
                String message = _socketIn.readLine();
                Token token;
                while(counter < Server.R){
                    try {
                        System.out.println("Waiting for stores " + Server.stores_counter);
                        token = Tokenizer.getToken(message);
                        if(token instanceof JoinStoreToken){
                            addStore(token);
                            counter++;
                        }

                        if(token instanceof JoinClientToken)
                            addClient(token);
                        message = _socketIn.readLine();
                        this.wait(10);

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void addClient(Token token){
        Server.add_client(((JoinClientToken) token)._name, _socketOut);
        _name = ((JoinClientToken) token)._name;
        _isClient = true;
    }
    public void removeClient(){
        Server.remove_client(_name);
    }

    public void addStore(Token token){
        System.out.println(((JoinStoreToken) token)._name);
        Server.add_store(((JoinStoreToken) token)._name,((JoinStoreToken) token)._port, _socketOut);
        _name = ((JoinStoreToken) token)._name;
        _isStore = true;
        (new File(Paths.get(System.getProperty("user.dir"), _name).toString())).mkdir();
    }

    public void removeStore(){
        Server.remove_store(_name);
        File index = new File(Paths.get(System.getProperty("user.dir"), _name).toString());
        String[]entries = index.list();
        for(String s: entries){
            File currentFile = new File(index.getPath(),s);
            currentFile.delete();
        }
        index.delete();
    }


}