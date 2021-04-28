package com.company;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import static com.company.Server.client_counter;

public class Client {

    private String hostname;
    private int cport;
    private String userName;
    private PrintWriter _writerC;
    private BufferedReader _readerC;
    private ArrayList<Integer> portsToSend;
    public int increment = 0;
    private String _filename;
    private long _filesize;
    private BufferedReader _fromStore = null;
    private PrintWriter _toStore = null;


    private class ReadThreadClient extends Thread {

        public void run() {
            try {
                Token token;
                String response;
                do {
                    response = _readerC.readLine();
                    String responseRaw = response.contains(":") ? response.split(":")[1] : "none";
                    token = Tokenizer.getToken(responseRaw);
                    if (token instanceof StoreToken)
                        storeTokenHandler(token);
                    if (token instanceof StoreToToken)
                        storeToTokenHandler(token);
                    if (token instanceof ACKToken)
                        ACKHandler(token);
                    else {
                        System.out.println(response);
                    }
                } while (!(token instanceof ExitToken));
            } catch (IOException ex) {
                System.out.println("Error reading from server: " + ex.getMessage());
                ex.printStackTrace();
            }
        }
    }// End of the ReadThread class

    private class WriteThreadClient extends Thread {
        private Socket _socket;
        private Client _client;

        public void run() {

            //Console console = System.console();
            Scanner console = new Scanner(System.in);
            // String userName = console.readLine("\nEnter your name: ");
            String text;// = console.nextLine();
            //_writer.println("JOIN_CLIENT " + text);
            do {
                text = console.nextLine();
                _writerC.println(text);
            } while (!text.equalsIgnoreCase("EXIT"));
            try {
                _socket.close();
            } catch (IOException ex) {
                System.out.println("Error writing to server: " + ex.getMessage());
            }
        }
    }// End of the WriterThread class

    public Client(String name, int cport, int timeout) {
        this.hostname = "localhost";
        this.cport = cport;
        this.userName = name;
        try {
            Socket socket = new Socket(hostname, cport);
            _readerC = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            _writerC = new PrintWriter(socket.getOutputStream(), true);
        } catch (IOException ex) {
            System.out.println("Error getting input stream: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    public synchronized void connect(){
        _writerC.println("JOIN_CLIENT "+userName);
    }

    public void disconnect(){
        _writerC.println("EXIT");
    }

    public synchronized void store(File file){
        try {
            _writerC.println("STORE " + file.getPath() + " " + Files.size(Paths.get(file.getPath())));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void storeTokenHandler(Token token) {
        _filename = ((StoreToken) token)._filename;
        _filesize = ((StoreToken) token)._filesize;
    }

    public void storeToTokenHandler(Token token) {
        portsToSend = ((StoreToToken) token)._portList;
        storeToStores();
    }

    public void storeToStores(){
        try {
            for (Integer port : portsToSend) {
                Socket socket_ = new Socket(hostname, port);
                _toStore = new PrintWriter(socket_.getOutputStream(), true);
                _fromStore = new BufferedReader(new InputStreamReader(socket_.getInputStream()));

                _toStore.println("STORE " + _filename + " " + _filesize);
                String message = _fromStore.readLine();
                if (Tokenizer.getToken(message) instanceof ACKToken){
                    System.out.println("ACK 1 confirmed");
                    _writerC.println("ack confimremd");
                }
                else
                    throw new Error("There is no confirmation form the store");

                sendFile(socket_);

                message = _fromStore.readLine();
                System.out.println(message);
                if (Tokenizer.getToken(message) instanceof ACKToken)
                    System.out.println("ACK confirmed after transfer");

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendFile(Socket socket){
        try {
            FileInputStream fis = null;
            BufferedInputStream bis = null;
            OutputStream os = null;
                System.out.println("Waiting...");
                try {
                    System.out.println("Accepted connection : " + socket);
                    // send file
                    File myFile = new File(_filename);
                    byte[] mybytearray = new byte[(int) _filesize];
                    fis = new FileInputStream(myFile);
                    bis = new BufferedInputStream(fis);
                    bis.read(mybytearray, 0, mybytearray.length);
                    os = socket.getOutputStream();
                    System.out.println("Sending " + _filename + "(" + mybytearray.length + " bytes)");
                    os.write(mybytearray, 0, mybytearray.length);
                    os.flush();
                    System.out.println("Done.");


                }catch(IOException e) {
                    e.printStackTrace();
                }finally {
                    //if (bis != null) bis.close();
                    //if (os != null) os.close();
                }
        }finally {
            System.out.println("Closing file stream");
        }
    }

    public void ACKHandler(Token token) {
        System.out.println("Received ACK from: " + token._req);
        _writerC.println("ack confimremd");
    }

    public void execute() {
            new ReadThreadClient().start();
            new WriteThreadClient().start();
    }

    void setUserName(String userName) {
        this.userName = userName;
    }

    String getUserName() {
        return this.userName;
    }


    public static void main(String[] args) {
        if (args.length < 2) return;

        String name = args[0];
        int port = Integer.parseInt(args[1]);

        Client client = new Client(name + Server._clientMap.size(), port, 400);
        client.connect();
        client.execute();

    }


}
