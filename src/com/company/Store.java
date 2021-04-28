package com.company;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class Store {

    private String hostname;
    private Socket _clientSocket;
    private Socket _storeSocket;
    private int port;
    private int cport;
    private String _storename;
    private PrintWriter _writer;
    private BufferedReader _reader;
    private BufferedReader _clientIn;
    private PrintWriter _clientOut;
    private static int increment = 0;
    private boolean update_process = false;
    private FileOutputStream fos = null;
    private BufferedOutputStream bos = null;
    private InputStream is = null;
    private ServerThreadStore serverThread = null;


    private class ReadThreadStore extends Thread {
        private Socket _socket;
        private Store _store;

        public ReadThreadStore(Socket socket, Store store) {
            this._socket = socket;
            this._store = store;

            try {
                InputStream input = socket.getInputStream();
                _reader = new BufferedReader(new InputStreamReader(input));
            } catch (IOException ex) {
                System.out.println("Error getting input stream: " + ex.getMessage());
                ex.printStackTrace();
            }
        }

        public void run() {
            try {
                Token token;
                String response = _reader.readLine();
                String responseRaw = response.contains(":") ? response.split(":")[1] : "none";
                do {
                    token = Tokenizer.getToken(responseRaw);
                    response = _reader.readLine();
                    responseRaw = response.contains(":") ? response.split(":")[1] : "none";
                    System.out.println("messageread: " + response);


                } while (true);
            } catch (IOException ex) {
                System.out.println("Error reading from server: " + ex.getMessage());
                ex.printStackTrace();
            }
        }
    }// End of the ReadThread class

    private class WriteThreadStore extends Thread {
        private Socket _socket;
        private Store _store;

        WriteThreadStore(Socket socket, Store store) {
            this._socket = socket;
            this._store = store;

            try {
                OutputStream output = socket.getOutputStream();
                _writer = new PrintWriter(output, true);
            } catch (IOException ex) {
                System.out.println("Error getting output stream: " + ex.getMessage());
                ex.printStackTrace();
            }
        }

        public void run() {

            //Console console = System.console();

            Scanner console = new Scanner(System.in);
            // String userName = console.readLine("\nEnter your name: ");
            String text;
            System.out.println("ENTER STORE NAME:");
            //text = console.nextLine();

            //_writer.println("JOIN_STORE " + text + " " + port);
            //_storename = text;
            do {
                while(update_process){}
                text = console.nextLine();
                _writer.println(text);
            } while (!text.equalsIgnoreCase("EXIT"));
            try {
                _socket.close();
            } catch (IOException ex) {
                System.out.println("Error writing to server: " + ex.getMessage());
            }
        }
    }// End of the WriterThread class

    private class ServerThreadStore extends Thread {
        private Socket _socket;
        private String _filename = null;
        private long _filesize = 0;
        private String _filepath = null;
        private BufferedReader _socketIn;
        private PrintWriter _socketOut;

        ServerThreadStore(Socket socket) throws IOException {
            _socket = socket;
            //_clientSocket.setSoTimeout(timeout);
            _socketIn = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            _socketOut = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
        }

        public void run() {
            try {


                /**
                 * Registering either user or dstore
                 */
                String message = _socketIn.readLine();
                Token token = Tokenizer.getToken(message);

                /**
                 * Start listening to the user/stores messages
                 */
                while (!(token instanceof ExitToken)) {
                    if (token instanceof StoreToken)
                        storeTokenHandler(token);
                    if (token instanceof FileSaveToken)
                        saveFile(token);
                    else {
                        if(update_process){
                            System.out.println("reading file");
                            openFileStrema();
                            while (update_process) {
                                readFileStream();
                            }
                            closeFileStream();
                        }else{
                            System.out.println("Message: " + message);
                        }
                    }
                    message = _socketIn.readLine();
                    token = Tokenizer.getToken(message);
                }
                _socket.close();
            } catch (IOException e) {
                System.out.println("Counght I/O Exception.");
                e.printStackTrace();
            } catch (NullPointerException e) {
                System.out.println("Cought null pointer exception. Probably invalid command. Remember of tokens");
                e.printStackTrace();

            }finally {
                //Server.remove_store(_filename);
            }
        }

        public void saveFile(Token token){
            ArrayList<Byte> file = ((FileSaveToken) token)._payload;
            Path path = Paths.get(System.getProperty("user.dir"), _storename, _filename);
            byte[] castFile = new byte[(int)_filesize];
            for(int i =0; i<file.size(); i++)
                castFile[i] = file.get(i);
            try (FileOutputStream fos = new FileOutputStream(path.toString())) {
                fos.write(castFile);
            }catch(IOException e){
                System.out.println("Error handeling file content transfer");
                e.printStackTrace();
                return;
            }
            _socketOut.println("STORE_ACK filename");
        }

        public void storeTokenHandler(Token token){
            System.out.println("STORE request has been received");
            System.out.println("STORE "+((StoreToken)token)._filename+" " + ((StoreToken)token)._filesize);
            System.out.println(_clientSocket);

            _filename = ((StoreToken)token)._filename;
            _filesize = ((StoreToken)token)._filesize;
            _filepath = Paths.get(System.getProperty("user.dir"), _storename, _filename).toString();
            sendACK();

            _writer.println("Store: " + _storename + " received stored request");



        }
        public void sendACK(){
            update_process = true;
            _socketOut.println("ACK");
            System.out.println("Send ACK");
        }

        public void storeACKComplete(){
            System.out.println("Store finished");
            update_process = false;
            _writer.println("STORE_COMPLETE");
        }

        public void sendStoreACK(String filename){
            _socketOut.println("STORE_ACK " + filename);
        }

        public void openFileStrema(){
            try {
                fos = new FileOutputStream(_filepath);
                bos = new BufferedOutputStream(fos);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }finally {
                System.out.println("Opening file stream");
            }
        }

        public void closeFileStream(){
            try {
                fos.close();
                bos.close();
                update_process=false;
                _writer.println("Closing file stream");
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                System.out.println("Closing file stream");
            }
        }

        public void readFileStream(){
            try {
                int bytesRead;
                int current = 0;
                byte [] mybytearray  = new byte [(int)_filesize];
                String filePath = Paths.get(System.getProperty("user.dir"), _storename, _filename).toString();
                System.out.println("Path: " + filePath);
                bytesRead = is.read(mybytearray,0,mybytearray.length);
                System.out.println("Reading: " + bytesRead);
                current = bytesRead;

                do {
                    bytesRead =
                            is.read(mybytearray, current, (mybytearray.length-current));
                    System.out.println(bytesRead);
                    if(bytesRead >= 0) current += bytesRead;
                } while(bytesRead > -1);
                System.out.println("File " + _filename
                        + " downloaded (" + current + " bytes read)");
                bos.write(mybytearray, 0 , current);
                bos.flush();
                update_process = false;
            } catch (IOException e) {
                e.printStackTrace();
            }finally{
                System.out.println("Finished receivng all file payload");
            }
        }

        public void readFile(Socket socket){
            int bytesRead;
            int current = 0;
            try {
                try {
                    System.out.println("Connecting..." +_filename + " "+  _filesize);
                    // receive file
                    byte [] mybytearray  = new byte [(int)_filesize];
                    bytesRead = is.readNBytes(mybytearray,0,(int)_filesize);
                    System.out.println("Reading: " + bytesRead);
                    current = bytesRead;
                    do {
                        bytesRead =
                                is.read(mybytearray, current, (mybytearray.length-current));
                        System.out.println(bytesRead);
                        if(bytesRead >= 0) current += bytesRead;
                    } while(bytesRead > -1);
                    System.out.println("File " + _filename
                            + " downloaded (" + current + " bytes read)");
                    bos.write(mybytearray, 0 , current);
                    bos.flush();

                }catch(IOException e){
                    e.printStackTrace();
                }
                finally {
                    //if (fos != null) fos.close();
                    //if (bos != null) bos.close();
                }
            }finally {
                System.out.println("End of receing file payload");
                update_process=false;
            }
        }
    }

    public void connect(){
        _writer.println("JOIN_STORE " + _storename + " " + port);
    }

    public void disconnect(){
        _writer.println("EXIT");
    }

    public void startListening(){

        try {
            _clientSocket =  new ServerSocket(port).accept();
            _clientIn = new BufferedReader(new InputStreamReader(_clientSocket.getInputStream()));
            _clientOut = new PrintWriter(new OutputStreamWriter(_clientSocket.getOutputStream()), true);
            is = _clientSocket.getInputStream();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Store(String name, String hostname, int port, int cport) {
        this._storename = name;
        this.hostname = hostname;
        this.cport = cport;
        this.port = port;
        try {
            _storeSocket = new Socket(hostname, cport);
            _reader = new BufferedReader(new InputStreamReader(_storeSocket.getInputStream()));
            _writer = new PrintWriter(_storeSocket.getOutputStream(), true);

            connect();
            startListening();
        } catch (IOException ex) {
            System.out.println("Error getting input stream: " + ex.getMessage());
            ex.printStackTrace();
        }
    }


    public void execute() {

        try {
            serverThread = new ServerThreadStore(_clientSocket);
            new WriteThreadStore(_storeSocket, this).start();
            while(true){
                serverThread.start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) return;

        String hostname = args[0];
        int port = Integer.parseInt(args[1]);

        Store store = new Store("blabla", hostname, 2222, port);
        store.execute();
    }




}
