package com.file_system;

import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
public class FileClient implements Runnable {
    private int serverPort;
    private String serverAddress;
    private String sourceFileName;
    private String destinationFileName;
    private String replication_status;
    private int append_number;
    private String nodeID;
    private LinkedHashMap<String, Integer> file_time_stamp;
    public FileClient(String address, String port, String sourceFileName, String destinationFileName, String replication_status, LinkedHashMap<String, Integer> time_stamp) {
        this.serverAddress = address;
        this.serverPort = Integer.parseInt(port);
        this.sourceFileName = sourceFileName;
        this.destinationFileName = destinationFileName;
        this.replication_status = replication_status;
        this.file_time_stamp = time_stamp;
    }
    public FileClient(String address, String port, String sourceFileName, String destinationFileName, String replication_status, int append_number, String nodeID) {
        this.serverAddress = address;
        this.serverPort = Integer.parseInt(port);
        this.sourceFileName = sourceFileName;
        this.destinationFileName = destinationFileName;
        this.replication_status = replication_status;
        this.append_number = append_number;
        this.nodeID = nodeID;
    }
    @Override
    public void run() {

        try {
            // System.out.println("Starting Client");
            // System.out.println("Server Address: " + this.serverAddress + " Server Port: " + this.serverPort);
            File file = new File(this.sourceFileName);
            if (!file.exists()) {
                System.out.println(sourceFileName + " - file does not exist. Aborting");
                return;
            }
            
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(this.serverAddress, this.serverPort), 5000);
            OutputStream outputStream = socket.getOutputStream(); // To send files
            File sourceFile = new File(this.sourceFileName);
            FileInputStream fileInputStream = new FileInputStream(sourceFile);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
            DataOutputStream dataOutputStream = new DataOutputStream(outputStream);

            dataOutputStream.writeUTF(this.replication_status);
            // Send file name:
            // System.out.println("DEBUG:- Sending file name");
            dataOutputStream.writeUTF(this.destinationFileName);
            // System.out.println("DEBUG:- Sent file name");
            if(this.replication_status.equals("append"))
            {
                dataOutputStream.writeLong(sourceFile.length());
                dataOutputStream.writeInt(append_number);
                dataOutputStream.writeUTF(nodeID);
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = bufferedInputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                    outputStream.flush();
                }
            }
            else if(!this.replication_status.equals("delete")) {
                // Send file size to the client first
                // System.out.println("DEBUG:- Sending file size");
                dataOutputStream.writeLong(sourceFile.length());
                // System.out.println("DEBUG:- Sent file size");
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                // Serialize the map
                objectOutputStream.writeObject(file_time_stamp);
                objectOutputStream.flush();
                // Sending files in chunks
                // System.out.println("DEBUG:- Sending file");
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = bufferedInputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                    outputStream.flush();
                }
                System.out.println("DEBUG:- Sent file");
            }

            // System.out.println("Closing connection");
            outputStream.close();
            dataOutputStream.close();
            bufferedInputStream.close();
            socket.close();
        } catch (IOException e) {
            System.err.println("Error while sending file: " + e);
        }
    }
    
}
