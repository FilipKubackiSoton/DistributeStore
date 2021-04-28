package com.company;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class StoreClientThread extends Thread{
    BufferedReader toClient ;
    BufferedWriter toStore;
    public StoreClientThread(BufferedReader toClient, BufferedWriter  toStore) {
        this.toClient = toClient;
        this.toStore = toStore;
    }

    public void run()
    {

    }
}
