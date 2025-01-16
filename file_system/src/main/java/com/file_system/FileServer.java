package com.file_system;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedHashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;

public class FileServer implements Runnable {
    private int port;
    private int serverPort;

    public FileServer(int port, int serverPort) {
        this.port = port;
        this.serverPort = serverPort;
    }

    public String createCustomXML(String rootElementName, String data) {
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            Document doc = docBuilder.newDocument();
            org.w3c.dom.Element rootElement = doc.createElement(rootElementName);
            doc.appendChild(rootElement);
            rootElement.appendChild(doc.createTextNode(data));
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(doc);
            StringWriter writer = new StringWriter();
            StreamResult result = new StreamResult(writer);
            transformer.transform(source, result);
            return writer.toString();
        } catch (ParserConfigurationException | TransformerException e) {
            System.err.println("Error creating XML: " + e.getMessage());
            return null;
        }
    }

    @Override
    public void run() {
        try {
            ServerSocket server = new ServerSocket(port);
            System.out.println("FileServer started on port " + port);

            while (true) {
                try (Socket clientSocket = server.accept();
                        DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream())) {

                    String replication_status = dataInputStream.readUTF();
                    String fileName = dataInputStream.readUTF();

                    if (replication_status.startsWith("JOB_ID:")) {
                        long fileSize = dataInputStream.readLong();
                        String jobId = replication_status.split(":")[1];
                        
                        File jobDir = new File("./data_storage/job_chunks/");
                        jobDir.mkdirs();
                        
                        File outputFile = new File("./data_storage/job_chunks/" + fileName);
                        
                        try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
                                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(
                                        fileOutputStream)) {

                            byte[] buffer = new byte[8192];
                            long totalBytesRead = 0;
                            int bytesRead;

                            while (totalBytesRead < fileSize && (bytesRead = dataInputStream.read(buffer)) != -1) {
                                bufferedOutputStream.write(buffer, 0, bytesRead);
                                totalBytesRead += bytesRead;
                            }

                            String pingMessage = createCustomXML("JOB_CHUNK_RECEIVED",
                                    fileName + ";" + jobId + ";" + fileSize);
                            byte[] message_buffer = pingMessage.getBytes();
                            DatagramPacket packet = new DatagramPacket(message_buffer, message_buffer.length,
                                    InetAddress.getByName("127.0.0.1"), this.serverPort);
                            DatagramSocket socket = new DatagramSocket(7002);
                            socket.send(packet);
                            socket.close();

                            System.out.println("Job chunk " + fileName + " received successfully for job " + jobId);
                        }
                    } else if (replication_status.equals("requested_file")) {
                        long fileSize = dataInputStream.readLong();
                        File outputFile = new File("./data_storage/local_file/" + fileName);
                        try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
                                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(
                                        fileOutputStream)) {

                            byte[] buffer = new byte[8192];
                            long totalBytesRead = 0;
                            int bytesRead;
                            ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream());
                            @SuppressWarnings("unchecked")
                            LinkedHashMap<String, Integer> file_time_stamp = (LinkedHashMap<String, Integer>) ois
                                    .readObject();

                            while (totalBytesRead < fileSize && (bytesRead = dataInputStream.read(buffer)) != -1) {
                                bufferedOutputStream.write(buffer, 0, bytesRead);
                                totalBytesRead += bytesRead;
                            }

                            String pingMessage = createCustomXML("GET_FILE_RECEIVED",
                                    fileName + ";" + replication_status + ";" + fileSize + ";" + file_time_stamp);
                            byte[] message_buffer = pingMessage.getBytes();
                            DatagramPacket packet = new DatagramPacket(message_buffer, message_buffer.length,
                                    InetAddress.getByName("127.0.0.1"), this.serverPort);
                            DatagramSocket socket = new DatagramSocket(7002);
                            socket.send(packet);
                            socket.close();
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    } else if (replication_status.equals("append")) {
                        long fileSize = dataInputStream.readLong();
                        int append_number = dataInputStream.readInt();
                        String nodeID = dataInputStream.readUTF();
                        File outputFile = new File("./data_storage/HyDFS/" + fileName);
                        boolean appendMode = outputFile.exists();
                        if (appendMode) {
                            try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile, appendMode);
                                    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(
                                            fileOutputStream)) {

                                byte[] buffer = new byte[8192];
                                long totalBytesRead = 0;
                                int bytesRead;

                                // Read the file in chunks and append if in append mode
                                while (totalBytesRead < fileSize && (bytesRead = dataInputStream.read(buffer)) != -1) {
                                    bufferedOutputStream.write(buffer, 0, bytesRead);
                                    totalBytesRead += bytesRead;
                                }
                                String pingMessage = createCustomXML("FILE_APPEND",
                                        fileName + ";" + nodeID + ";" + append_number);
                                byte[] message_buffer = pingMessage.getBytes();
                                DatagramPacket packet = new DatagramPacket(message_buffer, message_buffer.length,
                                        InetAddress.getByName("127.0.0.1"), this.serverPort);
                                DatagramSocket socket = new DatagramSocket(7002);
                                socket.send(packet);
                                socket.close();

                            } catch (IOException e) {
                                System.err.println("Error while appending the file: " + e.getMessage());
                            }
                        }
                    } else if (!replication_status.equals("delete")) {
                        long fileSize = dataInputStream.readLong();
                        File outputFile = new File("./data_storage/HyDFS/" + fileName);
                        try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
                                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(
                                        fileOutputStream)) {

                            byte[] buffer = new byte[8192];
                            long totalBytesRead = 0;
                            int bytesRead;
                            ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream());
                            LinkedHashMap<String, Integer> file_time_stamp = (LinkedHashMap<String, Integer>) ois
                                    .readObject();
                            // Read the file in chunks
                            while (totalBytesRead < fileSize && (bytesRead = dataInputStream.read(buffer)) != -1) {
                                bufferedOutputStream.write(buffer, 0, bytesRead);
                                totalBytesRead += bytesRead;
                            }
                            String pingMessage = createCustomXML("FILE_RECEIVED",
                                    fileName + ";" + replication_status + ";" + fileSize + ";" + file_time_stamp);
                            // System.out.println(pingMessage);
                            byte[] message_buffer = pingMessage.getBytes();
                            DatagramPacket packet = new DatagramPacket(message_buffer, message_buffer.length,
                                    InetAddress.getByName("127.0.0.1"), this.serverPort);
                            DatagramSocket socket = new DatagramSocket(7002);
                            socket.send(packet);
                            socket.close();

                            System.out.println("File " + fileName + " uploaded to HyDFS successfully.");
                        } catch (IOException | ClassNotFoundException e) {
                            System.err.println("Error writing file " + fileName + ": " + e.getMessage());
                        }
                    } else {
                        File deleteFile = new File("./data_storage/HyDFS/" + fileName);
                        if (deleteFile.delete()) {
                            System.out.println("File " + fileName + " deleted successfully");

                            String pingMessage = createCustomXML("FILE_RECEIVED", fileName + ";" + replication_status);
                            byte[] message_buffer = pingMessage.getBytes();
                            DatagramPacket packet = new DatagramPacket(message_buffer, message_buffer.length,
                                    InetAddress.getByName("127.0.0.1"), this.serverPort);
                            DatagramSocket socket = new DatagramSocket(7002);
                            socket.send(packet);
                            socket.close();

                        } else {
                            System.out.println("File " + fileName + " not deleted properly");
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Error handling client connection: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("FileServer error: " + e.getMessage());
        }
    }
}
