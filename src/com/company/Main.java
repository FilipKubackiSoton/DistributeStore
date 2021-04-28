package com.company;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;

public class Main {

    public static void main(String[] args) {
        int cport = 8989;
        int timeout = 400;
        ArrayList<Client> clientMap = new ArrayList<>();



	// write your code here

            new Thread() {
                public void run() {
                    Server server = new Server();
                    try {
                        server.startListening(cport);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }.start();

        for (int i = 0; i < 8; i++) {
            int finalI = i;
            if(i%2 == 0){
                new Thread() {
                    public void run() {
                        Store store = new Store("store"+ finalI, "localhost",2222+finalI, cport);
                        store.connect();
                        store.startListening();
                    }
                }.start();
            }else{
                new Thread() {
                    public void run() {
                        Client client = new Client("client"+ finalI, cport, timeout);
                        client.connect();
                        client.store(new File("tmp2.txt"));
                        client.disconnect();
                    }
                }.start();
            }


        }
    }

    public static void testClient(String name, int cport, int timeout) {
        Client client = null;

        try {
            client = new Client(name, cport, timeout);
            client.connect();
            client.store(new File("tmp1.txt"));
            //client.disconnect();
        } finally {
            if (client != null)
                try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
        }
    }
}

