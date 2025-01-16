package com.file_system;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.stream.Stream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class RainStorm {
    private static final Logger LOGGER = Logger.getLogger(RainStorm.class.getName());
    private static FileHandler fileHandler;
    final List<String> memberList = Collections.synchronizedList(new ArrayList<>()); // nodeId
    private final Node selfNode;
    private static String introducerIp;
    private static int introducerPort;
    private final Map<String, Long> pingMap = new ConcurrentHashMap<>();
    private final Map<String, Long> suspectedNodes = new ConcurrentHashMap<>();
    private static final int pingTimeout = 8000; // Timeout for ACK in milliseconds
    private static final int t_s = 2000; // Time period for suspicion before declaring failure
    private static final int k = 3; // Number of random nodes to ping
    private static Boolean isSuspicionEnabled = false;
    private double messageDropRate = 0.0;
    static Map<String, String> memberMap = new TreeMap(); // <NodeId, IP;Port>
    private static HashMap<String, Long> primary_files = new HashMap<>();; // file_name : file_size
    private static HashMap<String, Long> replicated_files = new HashMap<>(); // file_name : file_size
    private static List<String> successors = new ArrayList<>();
    private static List<String> predecessors = new ArrayList<>();
    private CopyOnWriteArrayList<String> ls_nodeList = new CopyOnWriteArrayList<>();
    private static Boolean isFileReceived = true;
    private CopyOnWriteArrayList<Boolean> append_request = new CopyOnWriteArrayList<>();
    private static HashMap<String, LinkedHashMap<String, Integer>> time_stamp = new HashMap<>();
    private static HashMap<String, Long> cacheMap = new HashMap<>();
    private static HashMap<String, String> cacheFileMap = new HashMap<>();
    private static final int cacheTimeout = 10000; // If a file is older than 10s, invalidate it from the cache
    LinkedHashMap<String, Integer> file_time_stamp_replica_1 = new LinkedHashMap<>();
    LinkedHashMap<String, Integer> file_time_stamp_replica_2 = new LinkedHashMap<>();
    private static final Scanner scanner = new Scanner(System.in);

    static {
        try {
            // Create a FileHandler for logging to a file
            fileHandler = new FileHandler("Rain_Storm.log", true);
            SimpleFormatter formatter = new SimpleFormatter();
            fileHandler.setFormatter(formatter);

            // Remove the default ConsoleHandler
            Logger rootLogger = Logger.getLogger("");
            Handler[] handlers = rootLogger.getHandlers();
            for (Handler handler : handlers) {
                if (handler instanceof ConsoleHandler) {
                    rootLogger.removeHandler(handler);
                }
            }

            // Add the FileHandler to the logger
            LOGGER.addHandler(fileHandler);
            LOGGER.setLevel(Level.ALL);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public RainStorm(Node selfNode) {
        this.selfNode = selfNode;
        LOGGER.info("RainStorm initialized for node: " + selfNode.id);
    }

    public Boolean isNotDropped() {
        Random random = new Random();
        double randomValue = random.nextDouble() * 100;

        // if (randomValue < messageDropRate) {
        // return false;
        // } else {
        // return true;
        // }

        return true;
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
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public Document convertStringToXMLDocument(String xmlString) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            InputStream is = new ByteArrayInputStream(xmlString.getBytes("UTF-8"));
            return builder.parse(is);
        } catch (ParserConfigurationException | SAXException | IOException e) {
            System.out.println("Error in convertStringToXMLDocument");
            e.printStackTrace();
            return null;
        }
    }

    public void joinIntroducer() {
        try {
            LOGGER.info("Attempting to join group through introducer: " + introducerIp + ":" + introducerPort);
            String message = selfNode.ipAddress + ";" + selfNode.port + ";" + selfNode.id;
            String xmlString = createCustomXML("JOIN", message);
            byte[] buffer = xmlString.getBytes();
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, InetAddress.getByName(introducerIp),
                    introducerPort);
            selfNode.socket.send(packet);

            // Receive membership list
            byte[] receiveBuffer = new byte[1024];
            DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
            selfNode.socket.receive(receivePacket);
            String membershipMessage = new String(receivePacket.getData(), 0, receivePacket.getLength());
            processInputMessage(membershipMessage, receivePacket.getAddress());
            System.out.println("Successfully joined group");
            LOGGER.info("Successfully joined group");
        } catch (IOException e) {
            System.err.println("Error joining introducer: " + e.getMessage());
            LOGGER.severe("Error joining introducer: " + e.getMessage());
        }
    }

    public void leaveGroup() {
        System.out.println("Leaving Group");
        broadcastMessage("REMOVE", this.selfNode.id);
        this.selfNode.socket.close();
    }

    public static void enableSuspicion() {
        System.out.println("Enabled Suspicion");
        isSuspicionEnabled = true;
    }

    public static void disableSuspicion() {
        System.out.println("Disabled Suspicion");
        isSuspicionEnabled = false;
    }

    private void updateNodeIncarnation(String nodeId, int newIncarnation) {
        for (String member : memberList) {
            if (member.startsWith(nodeId)) {
                String[] details = member.split(";");
                int currentIncarnation = Integer.parseInt(details[2]);
                if (newIncarnation > currentIncarnation) {
                    // Update the member list with new incarnation number
                    memberList.remove(member);
                    memberList.add(nodeId + ";" + details[0] + ";" + newIncarnation);
                    System.out.println("Updated incarnation number for " + nodeId + " to " + newIncarnation);
                }
                break;
            }
        }
    }

    public void pingSelectedNodes() {
        // System.out.println("Sending Pings");
        if (memberList.isEmpty())
            return;
        // Select k random nodes to ping
        List<String> randomNodes = selectKNeighbours();
        for (String randomNode : randomNodes) {
            if (!randomNode.equals(selfNode.id) && !pingMap.containsKey(randomNode)) { // Exclude nodes already being
                                                                                       // pinged
                pingMap.put(randomNode, System.currentTimeMillis());
                if (isNotDropped()) {
                    selfNode.sendPing(memberMap.get(randomNode));
                    // System.out.println("Sent PING to node: " + randomNode);
                    LOGGER.fine("Sent PING to node: " + randomNode);
                }
            }
        }
    }

    private List<String> selectKNeighbours() {
        List<String> randomNodes = new ArrayList<>();
        // If memberList has no other members
        if (memberList.size() <= 1) {
            return randomNodes;
        }

        // Get the index of the selfNode in the memberList
        int selfIndex = memberList.indexOf(selfNode.id);
        if (selfIndex == -1) {
            return randomNodes;
        }

        // Number of predecessors and successors
        int halfK = Math.min(k / 2, (memberList.size() - 1) / 2); // -1 to exclude selfNode
        // Select predecessors
        for (int i = 1; i <= halfK; i++) {
            int predecessorIndex = (selfIndex - i + memberList.size()) % memberList.size();
            if (!memberList.get(predecessorIndex).equals(selfNode.id)) {
                randomNodes.add(memberList.get(predecessorIndex));
            }
        }
        // Select successors
        for (int i = 1; i <= halfK; i++) {
            int successorIndex = (selfIndex + i) % memberList.size();
            if (!memberList.get(successorIndex).equals(selfNode.id)) {
                randomNodes.add(memberList.get(successorIndex));
            }
        }
        // If there are fewer than k nodes, ensure we get all available nodes (excluding
        // selfNode)
        if (randomNodes.size() < k) {
            for (String node : memberList) {
                if (!node.equals(selfNode.id) && !randomNodes.contains(node)) {
                    randomNodes.add(node);
                    if (randomNodes.size() == k)
                        break; // Stop once we reach k nodes
                }
            }
        }
        LOGGER.fine("Nodes Selected to PING " + randomNodes);
        return randomNodes;
    }

    public void checkForFailures() {
        long currentTime = System.currentTimeMillis();

        // Check for nodes that didn't respond
        pingMap.forEach((node, timestamp) -> {
            if (!node.equals("")) {
                if (currentTime - timestamp > pingTimeout) {
                    if (isSuspicionEnabled) {
                        if (!suspectedNodes.containsKey(node)) {
                            System.out.println("Node " + node + " is suspected (no ACK received).");
                            suspectedNodes.put(node, currentTime);
                            broadcastMessage("SUSPICION", node);
                            LOGGER.warning("Node " + node + " is suspected (no ACK received)");
                        }
                        pingMap.remove(node);
                    } else {
                        System.out.println("Node " + node + " has failed (no ACK received).");
                        pingMap.remove(node);
                        memberList.remove(node);
                        memberMap.remove(node);
                        broadcastMessage("FAIL", node);
                        LOGGER.warning("Node " + node + " has failed (no ACK received)");
                    }
                    if (!selfNode.isLeader) {
                        System.out.println("Calling update stage 2 assignment on failure from rainstorm.java");
                        selfNode.updateStage2AssignmentOnFailure(node);
                    }
                }
            }
        });

        List<String> nodesToRemove = new ArrayList<>();

        if (isSuspicionEnabled) {
            suspectedNodes.forEach((node, timestamp) -> {
                if (currentTime - timestamp > t_s) {
                    System.out.println("Node " + node + " has failed (confirmed after suspicion period).");
                    nodesToRemove.add(node);
                    memberList.remove(node);
                    memberMap.remove(node);
                    broadcastMessage("FAIL", node);
                }
            });
        }
        nodesToRemove.forEach(suspectedNodes::remove);
    }

    public void broadcastMessage(String action, String node) {
        try {
            String broadcastMessage = selfNode.createCustomXML(action, node);
            if ("INCARNATION".equals(action) && isSuspicionEnabled) {
                int incarnation = selfNode.incarnationNumber;
                // Get the incarnation number
                broadcastMessage = selfNode.createCustomXML("INCARNATION", node + "," + incarnation);
            }
            byte[] buffer = broadcastMessage.getBytes();

            // Broadcast to all nodes
            for (String member : memberList) {
                if (!member.equals(selfNode.id)) {
                    String[] memberDetails = memberMap.get(member).split(";");
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length,
                            InetAddress.getByName(memberDetails[0]),
                            Integer.parseInt(memberDetails[1]));
                    if (isNotDropped()) {
                        this.selfNode.socket.send(packet);
                    }
                }
            }

            // Notify the introducer
            DatagramPacket introducerPacket = new DatagramPacket(buffer, buffer.length,
                    InetAddress.getByName(introducerIp), introducerPort);
            this.selfNode.socket.send(introducerPacket);
            LOGGER.info("Broadcasted " + action + " message for node: " + node);
        } catch (IOException e) {
            System.err.println("Error broadcasting failure: " + e.getMessage());
            LOGGER.severe("Error broadcasting message: " + e.getMessage());
        }
    }

    /*
     * If the immediate successor changes, we need to inform the second replica to
     * delete this node's primary files
     * If the second successor changes, we need to inform the old second replica to
     * delete htis node's primary files
     */
    public void informSecondSuccessor(String secondSuccessorIp) {
        if (!primary_files.isEmpty()) {
            for (String file : primary_files.keySet()) {
                Thread fileClientThread = new Thread(
                        new FileClient(secondSuccessorIp, "5002", "data_storage/HyDFS/" + file, file, "delete",
                                time_stamp.get(file)));
                fileClientThread.start();
            }
        }
    }

    /*
     * If a node has a new predecessor, it should check its list of primary files
     * and see if any of the files fall into the new predecessors range
     * aka predecessor's id > file id
     */
    public void informNewPredecessor(String predecessorNodeId, String predecessorIp) {
        if (!primary_files.isEmpty()) {
            for (String file : primary_files.keySet()) {
                String fileHash = generateHashID(file);
                if (predecessorNodeId.compareTo(fileHash) > 0) {
                    Thread fileClientThread = new Thread(
                            new FileClient(predecessorIp, "5002", "data_storage/HyDFS/" + file, file, "primary",
                                    time_stamp.get(file)));
                    fileClientThread.start();

                    Long fileSize = primary_files.remove(file);
                    replicated_files.put(file, fileSize);
                    fileClientThread = new Thread(new FileClient(memberMap.get(successors.get(1)).split(";")[0], "5002",
                            "data_storage/HyDFS/" + file, file, "delete", time_stamp.get(file)));
                    fileClientThread.start();
                }
            }
        }
        if (!replicated_files.isEmpty()) {
            for (String file : replicated_files.keySet()) {
                String fileHash = generateHashID(file);
                if (predecessorNodeId.compareTo(fileHash) < 0) { // fileHash is greater than the new predecessorNodeId
                    Long fileSize = replicated_files.get(file);
                    primary_files.put(file, fileSize);
                    replicated_files.remove(file);
                }
            }
        }
    }

    public void identifySuccessorsPredecessors() {
        int selfIndex = memberList.indexOf(selfNode.id);
        String successorOne = memberList.get((selfIndex + 1) % memberList.size());
        String successorTwo = memberList.get((selfIndex + 2) % memberList.size());
        if (successors.size() > 0) {
            List<String> prevSuccessors = new ArrayList<>(successors);
            successors.set(0, successorOne);
            successors.set(1, successorTwo);

            if (!successorOne.equals(prevSuccessors.get(0)) || !successorTwo.equals(prevSuccessors.get(1))) {
                informSecondSuccessor(memberMap.get(prevSuccessors.get(1)).split(";")[0]);
            }
        } else {
            successors.add(successorOne);
            successors.add(successorTwo);
        }

        int prevIndex = (selfIndex - 1 + memberList.size()) % memberList.size();
        int secondPrevIndex = (selfIndex - 2 + memberList.size()) % memberList.size();
        if (predecessors.size() > 0) {
            List<String> prevPredecessors = new ArrayList<>(predecessors);
            predecessors.set(0, memberList.get(prevIndex));
            predecessors.set(1, memberList.get(secondPrevIndex));

            if (!predecessors.get(0).equals(prevPredecessors.get(0))) {
                informNewPredecessor(predecessors.get(0), memberMap.get(predecessors.get(0)).split(";")[0]);
            }
        } else {
            predecessors.add(memberList.get(prevIndex));
            predecessors.add(memberList.get(secondPrevIndex));
        }

    }

    // Generating NodeID using SHA - 1
    public static String generateHashID(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");

            byte[] hashBytes = md.digest(input.getBytes());

            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-1 algorithm not found", e);
        }
    }

    public void createHyDFS(String source, String destination) {
        Path sourceFile = Paths.get(source);
        Path destinationFile = Paths.get(destination);
        if (Files.exists(destinationFile)) {
            System.out.println("File already exists at the destination. Copy operation aborted.");
            return;
        }
        try (FileChannel sourceChannel = FileChannel.open(sourceFile, StandardOpenOption.READ);
                FileChannel destinationChannel = FileChannel.open(destinationFile, StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

            sourceChannel.transferTo(0, sourceChannel.size(), destinationChannel);
            // System.out.println("File copied successfully!");

        } catch (IOException e) {
            System.err.println("File copy failed: " + e.getMessage());
        }
    }

    public void getFromHyDFS(String source, String destination) {
        String destination_Hash = generateHashID(source);
        int index = Collections.binarySearch(memberList, destination_Hash);
        if (index < 0) {
            index = -(index + 1);
        }
        index = index % memberList.size();
        int replicaOffset = Math.abs((source + selfNode.id).hashCode()) % 3;
        index = (index + replicaOffset) % memberList.size();
        String node = memberList.get(index);
        index = index % memberList.size();
        String xmlMessage = selfNode.createCustomXML("GET_FILE", selfNode.id + ";" + source + ";" + destination);
        byte[] buffer = xmlMessage.getBytes();
        if (isFileReceived) {
            if (!cacheMap.containsKey(source)) {
                cacheMap.put(source, System.currentTimeMillis());
                cacheFileMap.put(source, destination);
            }
        }
        System.out.println("Searching in node " + node);
        try {
            String[] nodeIpPort = memberMap.get(node).split(";");
            InetAddress nodeAddress = InetAddress.getByName(nodeIpPort[0]);
            int nodePort = Integer.parseInt(nodeIpPort[1]);
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, nodeAddress, nodePort);
            selfNode.socket.send(packet);
            try {
                Thread.sleep(2000); // Wait for 2 seconds while other actions can continue
            } catch (InterruptedException e) {
                System.out.println("Interrupted while waiting.");
            }
        } catch (IOException e) {
            System.out.println("Error while communicating with node " + node);
        }
        if (!isFileReceived)

        {
            System.out.println("File " + source + " does not exist in HyDFS");
        }
        isFileReceived = true;
    }

    public void getfromreplica(String source, String destination, String nodeIP) {
        String xmlMessage = selfNode.createCustomXML("GET_FILE", selfNode.id + ";" + source + ";" + destination);
        byte[] buffer = xmlMessage.getBytes();
        try {
            String[] nodeIpPort = nodeIP.split(";");
            InetAddress nodeAddress = InetAddress.getByName(nodeIpPort[0]);
            int nodePort = Integer.parseInt(nodeIpPort[1]);
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, nodeAddress, nodePort);
            selfNode.socket.send(packet);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                System.out.println("Interrupted while waiting.");
            }
        } catch (IOException e) {
            System.out.println("Error while communicating with node " + nodeIP);
        }
        if (!isFileReceived) {
            System.out.println("File received" + source + " does not exist in HyDFS");
        }
        isFileReceived = true;
    }

    public void appendHyDFS(String source, String destination, int append_number) {
        // Send requests to all nodes
        String destination_Hash = generateHashID(destination);
        int index = Collections.binarySearch(memberList, destination_Hash);
        if (index < 0) {
            index = -(index + 1);
        }
        index = index % memberList.size();
        int replicaOffset = Math.abs((destination + selfNode.id).hashCode()) % 3;
        index = (index + replicaOffset) % memberList.size();
        String node = memberList.get(index);
        try {
            System.out
                    .println(selfNode.id + " Trying to append to file " + destination + " in " + memberList.get(index));
            String[] nodeIpPort = memberMap.get(memberList.get(index)).split(";");
            Thread fileClientThread = new Thread(
                    new FileClient(nodeIpPort[0], "5002", source, destination, "append", append_number,
                            selfNode.id));
            fileClientThread.start();
            try {
                Thread.sleep(2000); // Wait for 2 seconds while other actions can continue
            } catch (InterruptedException e) {
                System.out.println("Interrupted while waiting.");
            }
        } catch (Exception e) {
            System.out.println("Error while communicating with node " + node);
        }
        append_request.set(append_number, false);
    }

    public void getLocationOfFile(String file_name) {
        String search_file = generateHashID(file_name);
        String xmlMessage = selfNode.createCustomXML("FILE_CHECK", selfNode.id + ";" + file_name);
        byte[] buffer = xmlMessage.getBytes();
        // Send requests to all nodes
        for (String node : memberList) {
            try {
                String[] nodeIpPort = memberMap.get(node).split(";");
                InetAddress nodeAddress = InetAddress.getByName(nodeIpPort[0]);
                int nodePort = Integer.parseInt(nodeIpPort[1]);

                DatagramPacket packet = new DatagramPacket(buffer, buffer.length, nodeAddress, nodePort);
                selfNode.socket.send(packet);
            } catch (IOException e) {
                System.out.println("Error while communicating with node " + node);
            }
        }

        // Now, wait for 2 seconds while startServer runs in the background
        try {
            Thread.sleep(2000); // Wait for 2 seconds while other actions can continue
        } catch (InterruptedException e) {
            System.out.println("Interrupted while waiting.");
        }

        // Continue the file search after waiting
        System.out.println("Completed search for file locations for file " + file_name + " with ID " + search_file);
        // Print results or handle further actions here (e.g., list the found nodes)
        for (String location : ls_nodeList) {
            System.out.println("Node ID: " + location + " Node Address: " + memberMap.get(location));
        }
        ls_nodeList.clear();
    }

    public void initiate_multi_append(String destinationFileName) {
        int N = 10;

        Set<String> selectedNodes = new HashSet<>();
        Random random = new Random();
        Set<String> selectedFiles = new HashSet<>();
        while (selectedNodes.size() < N) {
            int randomIndex = random.nextInt(memberList.size());
            selectedNodes.add(memberList.get(randomIndex));
        }
        ArrayList<String> nodesSelected = new ArrayList<>(selectedNodes);

        while (selectedFiles.size() < N) {
            int randomIndex = random.nextInt(20) + 1;
            selectedFiles.add("business_" + randomIndex + ".txt");
        }
        ArrayList<String> filesSelected = new ArrayList<>(selectedFiles);
        System.out.println("Nodes Selected: " + nodesSelected);
        System.out.println("Files Selected: " + filesSelected);
        for (int i = 0; i < N; i++) {
            try {
                String xmlString = createCustomXML("PERFORM_APPEND", filesSelected.get(i) + ";" + destinationFileName);
                byte[] buffer = xmlString.getBytes();
                String[] targetDetails = memberMap.get(nodesSelected.get(0)).split(";");
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length,
                        InetAddress.getByName(targetDetails[0]),
                        Integer.parseInt(targetDetails[1]));
                this.selfNode.socket.send(packet);
            } catch (Exception e) {
                System.out.println("Error in initiating multi_append");
            }
        }
        System.out.println("Multi append initiated successfully");
    }

    public void MergeData(String file_name) {
        getfromreplica(file_name, "merge_replica_1.txt", memberMap.get(successors.get(0)));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        getfromreplica(file_name, "merge_replica_2.txt", memberMap.get(successors.get(1)));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LinkedHashMap<String, Integer> file_time_stamp = new LinkedHashMap<>();
        file_time_stamp = time_stamp.get(file_name);
        boolean isMergeRequired = true;
        System.out.println(file_time_stamp);
        System.out.println(file_time_stamp_replica_1);
        System.out.println(file_time_stamp_replica_2);
        for (String s : file_time_stamp.keySet()) {
            if (file_time_stamp.get(s) != file_time_stamp_replica_1.get(s)
                    || file_time_stamp.get(s) != file_time_stamp_replica_2.get(s)) {
                isMergeRequired = true;
                time_stamp.get(file_name).replace(successors.get(0), file_time_stamp_replica_1.get(successors.get(0)));
                time_stamp.get(file_name).replace(successors.get(1), file_time_stamp_replica_2.get(successors.get(0)));
            }
        }
        if (isMergeRequired) {
            String[] nodeFiles = {
                    "data_storage/HyDFS/" + file_name, "data_storage/local_file/merge_replica_1.txt",
                    "data_storage/local_file/merge_replica_2.txt"
            };
            List<BufferedReader> readers = new ArrayList<>();
            List<String> mergedContent = new ArrayList<>();

            // Initialize BufferedReaders for each file
            try {
                for (String filePath : nodeFiles) {
                    readers.add(new BufferedReader(new FileReader(filePath)));
                }

                String line1 = readers.get(0).readLine();
                String line2 = readers.get(1).readLine();
                String line3 = readers.get(2).readLine();

                // Step 1: Read initial common lines from all files (including blank lines)
                while (line1 != null && line2 != null && line3 != null && line1.equals(line2) && line1.equals(line3)) {
                    mergedContent.add(line1);
                    line1 = readers.get(0).readLine();
                    line2 = readers.get(1).readLine();
                    line3 = readers.get(2).readLine();
                }
                if (line1 != null) {
                    mergedContent.add(line1);
                }
                if (line2 != null) {
                    mergedContent.add(line2);
                }
                if (line3 != null) {
                    mergedContent.add(line3);
                }

                while ((line1 = readers.get(0).readLine()) != null) {
                    mergedContent.add(line1);
                }
                while ((line2 = readers.get(1).readLine()) != null) {
                    mergedContent.add(line2);
                }
                while ((line3 = readers.get(2).readLine()) != null) {
                    mergedContent.add(line3);
                }

                // Step 3: Write the merged content (with all lines) to a new file
                try (BufferedWriter writer = new BufferedWriter(new FileWriter("data_storage/HyDFS/" + file_name))) {
                    // Write all lines, preserving order, including blank lines
                    for (String line : mergedContent) {
                        writer.write(line);
                        writer.newLine();
                    }
                }
                System.out.println("Files merged successfully!");

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                // Close all readers
                try {
                    for (BufferedReader reader : readers) {
                        reader.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            String successor_1 = memberMap.get(successors.get(0)).split(";")[0];
            String successor_2 = memberMap.get(successors.get(1)).split(";")[0];
            Thread fileClientThread = new Thread(new FileClient(successor_1, "5002",
                    "data_storage/HyDFS/" + file_name, "merge_" + file_name, "replica", time_stamp.get(file_name)));
            fileClientThread.start();
            File deleteFile = new File("data_storage/local_file/merge_replica_1.txt");
            if (deleteFile.delete()) {
                System.out.println("File merge_replica_1.txt" + " deleted successfully");
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Thread fileClientThread_2 = new Thread(new FileClient(successor_2, "5002",
                    "data_storage/HyDFS/" + file_name, "merge_" + file_name, "replica", time_stamp.get(file_name)));
            fileClientThread_2.start();
            File deleteFile_2 = new File("data_storage/local_file/merge_replica_2.txt");
            if (deleteFile_2.delete()) {
                System.out.println("File merge_replica_2.txt" + " deleted successfully");
            }
        }
    }

    public void checkReplication() {
        String action = "SUCCESSOR_PULL";
        String data = "";
        for (String key : primary_files.keySet()) {
            data = data.concat(key + ";" + primary_files.get(key) + ",");
        }
        if (!replicated_files.isEmpty()) {
            for (String key : replicated_files.keySet()) {
                data = data.concat(key + ";" + replicated_files.get(key) + ",");
            }
        }
        if (data.length() > 0) {
            data = data.substring(0, data.length() - 1);
        }

        String successorXml = selfNode.createCustomXML(action, data);
        byte[] buffer = successorXml.getBytes();
        try {
            for (String node : predecessors) {
                String[] nodeIpPort = memberMap.get(node).split(";");
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length,
                        InetAddress.getByName(nodeIpPort[0]),
                        Integer.parseInt(nodeIpPort[1]));
                this.selfNode.socket.send(packet);
            }
        } catch (IOException e) {
            System.out.println("Error while sending message in checkReplication");
        }
    }

    public static ArrayList<String> identifyFilesToReplicate(Map<String, String> successorFiles,
            InetAddress successorIp) {
        ArrayList<String> fileIds = new ArrayList<>();
        if (!primary_files.isEmpty()) {

            if (!successorFiles.isEmpty()) {
                for (String fileId : primary_files.keySet()) {
                    if (!successorFiles.containsKey(fileId)) {
                        fileIds.add(fileId);
                    }
                }
            } else {
                for (String fileId : primary_files.keySet()) {
                    fileIds.add(fileId);
                }
            }
        }
        if (!fileIds.isEmpty()) {
            for (String file : fileIds) {
                Thread fileClientThread = new Thread(new FileClient(successorIp.getHostAddress(), "5002",
                        "data_storage/HyDFS/" + file, file, "replica", time_stamp.get(file)));
                fileClientThread.start();
            }
        }
        return fileIds;
    }

    public void checkForInvalidCache() {
        long currentTime = System.currentTimeMillis();
        if (!cacheMap.isEmpty()) {
            for (String file : cacheMap.keySet()) {
                if (currentTime - cacheMap.get(file) > cacheTimeout) {
                    cacheMap.remove(file);
                    File deleteFile = new File("./data_storage/local_file/" + cacheFileMap.get(file));
                    if (deleteFile.delete()) {
                        System.out.println("Cache for file: " + file + " is successfully invalidated");
                    }
                }
            }
        }
    }

    public void sendCommandToLeader(String input) {
        // Get operations from user first
        String[] operations = promptForOperations();
        if (operations == null) {
            System.out.println("Operation cancelled or invalid inputs");
            return;
        }

        // Parse the original command to get filename and tasks
        String[] commands = input.split(" ");
        String hyDFSFileName = commands[3];
        int numTasks = commands.length > 4 ? Integer.parseInt(commands[4]) : 3;

        // Create the complete command with user-provided operations
        String operationMessage = String.join(" ", new String[] {
                "RAINSTORM",
                operations[0], // operation1
                operations[1], // operation2
                hyDFSFileName,
                String.valueOf(numTasks)
        });

        try {
            String[] leaderDetails = memberMap.get(selfNode.leader_id).split(";");
            String leaderIp = leaderDetails[0];
            int leaderPort = Integer.parseInt(leaderDetails[1]);

            String xmlMessage = selfNode.createCustomXML("RAINSTORM_OPERATION", operationMessage);
            byte[] buffer = xmlMessage.getBytes();
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length,
                    InetAddress.getByName(leaderIp), leaderPort);
            selfNode.socket.send(packet);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String[] promptForOperations() {
        String[] operations = new String[2];

        System.out.println("\n=== Operation 1 Configuration ===");
        System.out.println("Available operation types:");
        System.out.println(
                "1. FILTER:pattern");
        System.out.println("2. TRANSFORM:operation");
        System.out.println("3. FILTERED_TRANSFORM:pattern:operation");
        System.out.println("4. AGGREGATE:function:field");
        System.out.println(
                "\nFor transform operations, available functions are: uppercase, lowercase, trim, splitintowords");
        System.out.println("For aggregate operations, available functions are: sum, count, max, min");

        System.out.print("\nEnter Operation 1 (e.g., FILTER:error): ");
        operations[0] = scanner.nextLine().trim();

        System.out.println("\n=== Operation 2 Configuration ===");
        System.out.print("Enter Operation 2: ");
        operations[1] = scanner.nextLine().trim();

        // Validate operations
        if (!validateOperation(operations[0]) || !validateOperation(operations[1])) {
            if (!validateOperation(operations[0])) {
                System.out.println("Invalid operation format! - 1");
            }
            if (!validateOperation(operations[1])) {
                System.out.println("Invalid operation format! - 2");
            }
            return null;
        }

        System.out.println("\nConfigured Operations:");
        System.out.println("Operation 1: " + operations[0]);
        System.out.println("Operation 2: " + operations[1]);
        System.out.print("\nProceed with these operations? (y/n): ");

        String confirm = scanner.nextLine().trim().toLowerCase();
        if (!confirm.equals("y")) {
            return null;
        }

        return operations;
    }

    private boolean validateOperation(String operation) {
        if (operation == null || operation.trim().isEmpty()) {
            System.out.println("Debug: Operation is null or empty");
            return false;
        }

        String[] parts = operation.split(":");
        if (parts.length < 2) {
            System.out.println("Debug: Not enough parts in operation");
            return false;
        }

        String operationType = parts[0].toUpperCase();
        System.out.println("Debug: Operation type: " + operationType);

        switch (operationType) {
            case "FILTER":
                return parts.length == 2 && !parts[1].trim().isEmpty();

            case "TRANSFORM":
                if (parts.length < 2) {
                    System.out.println("Debug: Transform operation missing value");
                    return false;
                }

                String transformOperation = operation.substring(operation.indexOf(":") + 1);
                System.out.println("Debug: Transform operation: " + transformOperation);

                // Check for select transform
                if (transformOperation.toLowerCase().startsWith("select:")) {
                    String columns = transformOperation.substring(7).trim();
                    System.out.println("Debug: Select columns: " + columns);
                    return !columns.isEmpty() && columns.split(",").length > 0;
                }

                // Check for other transform types
                String transformType = transformOperation.toLowerCase().trim();
                System.out.println("Debug: Transform type: " + transformType);
                return Arrays.asList("uppercase", "lowercase", "trim", "splitintowords")
                        .contains(transformType);

            case "COLUMN_FILTER":
                // Format should be COLUMN_FILTER:columnName:value
                return parts.length == 3 &&
                        !parts[1].trim().isEmpty() && // column name not empty
                        !parts[2].trim().isEmpty(); // value not empty

            case "AGGREGATE":
                // AGGREGATE is a simple operator with no parameters needed
                return true;

            default:
                System.out.println("Debug: Unknown operation type: " + operationType);
                return false;
        }
    }

    private void processInputMessage(String membershipMessage, InetAddress senderIp) {
        Document xmlDocument = convertStringToXMLDocument(membershipMessage);
        String[] nodes;
        if (xmlDocument != null) {
            String messageType = xmlDocument.getDocumentElement().getNodeName();
            nodes = xmlDocument.getDocumentElement().getTextContent().split(",");
            switch (messageType) {
                case "LEADER":
                    String leader_id = xmlDocument.getDocumentElement().getTextContent();
                    selfNode.setLeader(leader_id);
                    break;
                case "RAINSTORM_OPERATION":
                    selfNode.processRainStormOperation(membershipMessage);
                    break;
                case "ADD":
                    for (String s : nodes) {
                        if (!s.equals("")) {
                            String nodeDetails[] = s.split(";");
                            memberList.add(nodeDetails[2]);
                            memberMap.put(nodeDetails[2], nodeDetails[0].concat(";").concat(nodeDetails[1]));
                        }
                    }
                    memberList.sort(null);
                    // Debug
                    String memberListString = "Length: " + memberList.size();
                    for (String member : memberList) {
                        memberListString += " " + member;
                    }
                    identifySuccessorsPredecessors();
                    LOGGER.info("Received membership list. Size: " + memberList.size());
                    break;
                case "PING":
                    String node = xmlDocument.getDocumentElement().getTextContent();
                    selfNode.receivePing(memberMap.get(node), isNotDropped());
                    LOGGER.fine("Received PING from node: " + node);
                    break;
                case "ACK":
                    String sender = xmlDocument.getDocumentElement().getTextContent();
                    pingMap.remove(sender);
                    LOGGER.fine("Received ACK from node: " + sender);
                    break;
                case "FAIL":
                    String failedNode = xmlDocument.getDocumentElement().getTextContent();
                    System.out.println("Node " + failedNode + " has failed. Removing from membership list.");
                    memberList.remove(failedNode);
                    suspectedNodes.remove(failedNode);
                    memberMap.remove(failedNode);
                    identifySuccessorsPredecessors();
                    LOGGER.warning("Node " + failedNode + " has failed and been removed from membership list");
                    selfNode.updateStage2AssignmentOnFailure(failedNode);
                    break;
                case "SUSPICION":
                    if (isSuspicionEnabled) {
                        String suspectedNode = xmlDocument.getDocumentElement().getTextContent();
                        System.out.println("Node " + suspectedNode + " is suspected.");
                        suspectedNodes.put(suspectedNode, System.currentTimeMillis());
                        LOGGER.warning("Node " + suspectedNode + " is suspected");
                    }
                    break;
                case "REMOVE":
                    memberList.removeAll(Arrays.asList(nodes));
                    for (String key : nodes) {
                        memberMap.remove(key);
                    }
                    // Debug
                    memberListString = "Length: " + memberList.size();
                    for (String member : memberList) {
                        memberListString += " " + member;
                    }
                    identifySuccessorsPredecessors();
                    System.out.println("Member List after Add: " + memberListString);
                    System.out.println("Removed nodes");
                    break;
                case "UPDATE_DROP_RATE":
                    messageDropRate = Double.parseDouble(xmlDocument.getDocumentElement().getTextContent());
                    break;
                case "ENABLE":
                    enableSuspicion();
                    break;
                case "DISABLE":
                    disableSuspicion();
                    break;
                case "INCARNATION":
                    if (isSuspicionEnabled) {
                        String[] data = xmlDocument.getDocumentElement().getTextContent().split(",");
                        String nodeId = data[0];
                        int newIncarnation = Integer.parseInt(data[1]);
                        updateNodeIncarnation(nodeId, newIncarnation);
                    }
                    break;
                case "FILE_RECEIVED":
                    String[] data = xmlDocument.getDocumentElement().getTextContent().split(";");
                    String fileName = data[0];
                    // Handle stage1 chunk files first
                    if (fileName.contains("stage1_chunk")) {
                        selfNode.handleChunkFileReceived(fileName);
                    } else if (fileName.contains("merge")) {
                        File deleteFile = new File("./data_storage/HyDFS/" + fileName.split("_")[1]);
                        if (deleteFile.delete()) {
                            System.out.println("File " + fileName.split("_")[1] + " deleted successfully");
                        }
                        try {
                            // Read content from the source file
                            String content = new String(
                                    Files.readAllBytes(Paths.get("./data_storage/HyDFS/" + fileName)));

                            // Write the content to the destination file
                            Files.write(Paths.get("./data_storage/HyDFS/" + fileName.split("_")[1]),
                                    content.getBytes());

                            File deleteFile_1 = new File("./data_storage/HyDFS/" + fileName);
                            if (deleteFile_1.delete()) {
                                System.out.println("File " + fileName + " deleted successfully");
                            }
                            System.out.println("File content has been successfully copied.");
                        } catch (IOException e) {
                            System.out.println("An error occurred: " + e.getMessage());
                            e.printStackTrace();
                        }
                        data[0] = data[0].split("_")[1];
                    }

                    if (data[1].compareTo("primary") == 0) {
                        primary_files.put(data[0], Long.parseLong(data[2]));
                        if (replicated_files.containsKey(data[0])) {
                            replicated_files.remove(data[0]);
                        }
                        if (!time_stamp.containsKey(data[0])) {
                            LinkedHashMap<String, Integer> file_time_stamp = new LinkedHashMap<>();
                            file_time_stamp.put(selfNode.id, 1);
                            file_time_stamp.put(successors.get(0), 1);
                            file_time_stamp.put(successors.get(1), 1);
                            time_stamp.put(data[0], file_time_stamp);
                        } else {
                            ArrayList<String> toRemove = new ArrayList<>();
                            for (Map.Entry<String, Integer> entry : time_stamp.get("File_Name").entrySet()) {
                                if (entry.getKey() == selfNode.id) {
                                    break;
                                }
                                toRemove.add(entry.getKey());
                            }
                            for (String s : toRemove) {
                                time_stamp.get(data[0]).remove(s);
                            }
                            if (time_stamp.get(data[0]).size() < 3
                                    && time_stamp.get(data[0]).size() < memberList.size()) {
                                time_stamp.get(data[0]).put(successors.get(2),
                                        time_stamp.get("File_Name").get(selfNode.id));
                            }
                        }
                    } else if (data[1].compareTo("replica") == 0) {
                        replicated_files.put(data[0], Long.parseLong(data[2]));
                        if (primary_files.containsKey(data[0])) {
                            primary_files.remove(data[0]);
                        }
                        String[] pairs = data[3].replaceAll("[{}]", "").split(", ");
                        LinkedHashMap<String, Integer> file_time_stamp = new LinkedHashMap<>();
                        for (String pair : pairs) {
                            String[] keyValue = pair.split("=");
                            String key = keyValue[0];
                            Integer value = Integer.parseInt(keyValue[1]);
                            file_time_stamp.put(key, value);
                        }
                        time_stamp.put(data[0], file_time_stamp);
                    } else if (data[1].compareTo("delete") == 0) {
                        if (primary_files.containsKey(data[0])) {
                            primary_files.remove(data[0]);
                        }
                        if (replicated_files.containsKey(data[0])) {
                            replicated_files.remove(data[0]);
                        }
                        time_stamp.remove(data[0]);
                    }
                    break;
                case "SUCCESSOR_PULL":
                    try {
                        String[] xmlData = xmlDocument.getDocumentElement().getTextContent().split(",");
                        Map<String, String> fileData = new TreeMap<>();
                        if (xmlData.length > 0 && !xmlData[0].equals("")) {
                            for (String pair : xmlData) {
                                String[] tmp = pair.split(";");
                                fileData.put(tmp[0], tmp[1]);
                            }
                        }
                        identifyFilesToReplicate(fileData, senderIp);
                    } catch (Exception e) {
                        System.out.println("Error in SUCCESSOR_PULL");
                        System.out.println(e);
                    }
                    break;
                case "FILE_CHECK":
                    try {
                        String[] xmlData = xmlDocument.getDocumentElement().getTextContent().split(";");
                        String sendingNode = xmlData[0];
                        String file_name = xmlData[1];
                        String isFilePresent = "";
                        if (primary_files.containsKey(file_name) || replicated_files.containsKey(file_name)) {
                            isFilePresent += "Yes";
                        } else {
                            isFilePresent += "No";
                        }
                        String xml_message = selfNode.createCustomXML("FILE_CHECK_RESPONSE",
                                selfNode.id + ";" + isFilePresent);
                        byte[] buffer = xml_message.getBytes();
                        String[] nodeIpPort = memberMap.get(sendingNode).split(";");
                        DatagramPacket packet;

                        packet = new DatagramPacket(buffer, buffer.length,
                                InetAddress.getByName(nodeIpPort[0]),
                                Integer.parseInt(nodeIpPort[1]));
                        this.selfNode.socket.send(packet);
                    } catch (Exception e) {
                        System.out.println("Error in File_Check");
                        e.printStackTrace();
                    }
                    break;
                case "FILE_CHECK_RESPONSE":
                    String[] xml_data = xmlDocument.getDocumentElement().getTextContent().split(";");
                    if (xml_data[1].equals("Yes"))
                        ls_nodeList.add(xml_data[0]);
                    break;
                default:
                    System.out.println("Received unknown message type: " + messageType);
                    break;

                case "GET_FILE":
                    try {
                        String[] xml_Data = xmlDocument.getDocumentElement().getTextContent().split(";");
                        // node id;source;destination;
                        if (primary_files.containsKey(xml_Data[1]) || replicated_files.containsKey(xml_Data[1])) {
                            String[] nodeIpPort = memberMap.get(xml_Data[0]).split(";");
                            String source = "data_storage/HyDFS/" + xml_Data[1];
                            String destinationFileName = xml_Data[2];
                            Thread fileClientThread = new Thread(
                                    new FileClient(nodeIpPort[0], "5002", source, destinationFileName,
                                            "requested_file", time_stamp.get(xml_Data[1])));
                            fileClientThread.start();
                        }
                    } catch (Exception e) {
                        System.out.println(e + " GET_FILE");
                    }
                    break;
                case "GET_FILE_RECEIVED":
                    try {
                        String[] xml_Data = xmlDocument.getDocumentElement().getTextContent().split(";");
                        System.out.println("Received file " + xml_Data[0] + ". Stored in local_file_directory");
                        isFileReceived = true;
                        if (xml_Data[0].equals("merge_replica_1.txt")) {
                            String[] pairs = xml_Data[3].replaceAll("[{}]", "").split(", ");

                            for (String pair : pairs) {
                                String[] keyValue = pair.split("=");
                                String key = keyValue[0];
                                Integer value = Integer.parseInt(keyValue[1]);
                                file_time_stamp_replica_1.put(key, value);
                            }
                        }

                        if (xml_Data[0].equals("merge_replica_2.txt")) {
                            String[] pairs = xml_Data[3].replaceAll("[{}]", "").split(", ");

                            for (String pair : pairs) {
                                String[] keyValue = pair.split("=");
                                String key = keyValue[0];
                                Integer value = Integer.parseInt(keyValue[1]);
                                file_time_stamp_replica_2.put(key, value);
                            }
                        }

                    } catch (Exception e) {
                        System.out.println("Error in GET_FILE_RECEIVED");
                        System.out.println(e);
                    }
                    break;
                case "FILE_APPEND":
                    try {
                        String[] xml_Data = xmlDocument.getDocumentElement().getTextContent().split(";");
                        System.out.println("Append to " + xml_Data[0] + " successfully received in " + selfNode.id);
                        String append_number = xml_Data[2];
                        String nodeID = xml_Data[1];
                        time_stamp.get(xml_Data[0]).replace(selfNode.id,
                                time_stamp.get(xml_Data[0]).get(selfNode.id) + 1);
                        String xml_message = selfNode.createCustomXML("FILE_APPEND_SUCCESSFUL",
                                selfNode.id + ";" + append_number);
                        byte[] buffer = xml_message.getBytes();
                        String[] nodeIpPort = memberMap.get(nodeID).split(";");
                        DatagramPacket packet;
                        packet = new DatagramPacket(buffer, buffer.length,
                                InetAddress.getByName(nodeIpPort[0]),
                                Integer.parseInt(nodeIpPort[1]));
                        this.selfNode.socket.send(packet);
                    } catch (Exception e) {
                        System.out.println("Error in FILE_APPEND");
                        e.printStackTrace();
                    }
                    break;
                case "FILE_APPEND_SUCCESSFUL":

                    try {
                        String[] xml_Data = xmlDocument.getDocumentElement().getTextContent().split(";");
                        System.out.println("Append successful to" + xml_Data[0]);
                        append_request.set(Integer.parseInt(xml_Data[1]), false);
                    } catch (Exception e) {
                        System.out.println(e + "FILE_APPEND_SUCCESSFUL");
                    }
                    break;
                case "PERFORM_APPEND":
                    try {
                        String[] xml_Data = xmlDocument.getDocumentElement().getTextContent().split(";");
                        String source = "data_storage/local_file/" + xml_Data[0];
                        String destinationFileName = xml_Data[1];
                        if (append_request.size() > 0) {
                            while (append_request.get(append_request.size() - 1)) {
                                // System.out.println("Stuck in while loop");
                            }
                        }
                        append_request.add(true);
                        new Thread(
                                () -> appendHyDFS(source, destinationFileName, append_request.size() - 1))
                                .start();
                    } catch (Exception e) {
                        System.out.println(e + " PERFORM_APPEND");
                    }
                    break;
                case "START_MERGE":
                    try {
                        String[] xml_Data = xmlDocument.getDocumentElement().getTextContent().split(";");
                        String destinationFileName = xml_Data[0];
                        new Thread(
                                () -> MergeData(destinationFileName))
                                .start();
                    } catch (Exception e) {
                        System.out.println(e + " START_MERGE RECIPIENT EXCEPTION");
                    }
                    break;
                case "STAGE_ASSIGNMENT":
                    System.out
                            .println("Received stage assignment: " + xmlDocument.getDocumentElement().getTextContent());
                    selfNode.handleStageAssignment(xmlDocument.getDocumentElement().getTextContent());
                    break;
                case "TASK_STATUS":
                    selfNode.handleTaskStatus(xmlDocument.getDocumentElement().getTextContent());
                    break;
                case "JOB_CHUNK_RECEIVED":
                    String[] chunkInfo = xmlDocument.getDocumentElement().getTextContent().split(";");
                    String chunkFileName = chunkInfo[0];
                    String jobId = chunkInfo[1];
                    String status = chunkInfo.length > 3 ? chunkInfo[3] : "RECEIVED";
                    if (!"SKIPPED".equals(status)) {
                        try {
                            selfNode.startChunkProcessing(chunkFileName);
                        } catch (Exception e) {
                            System.err.println("Chunk processing interrupted: " + e.getMessage());
                            Thread.currentThread().interrupt();
                        }
                    }
                    break;
                case "STAGE2_RESULT":
                    if (selfNode.isLeader) {
                        String[] resultParts = xmlDocument.getDocumentElement().getTextContent().split(";");
                        int resultJobId = Integer.parseInt(resultParts[0]);
                        String uniqueId = resultParts[1];
                        String filename = resultParts[2];
                        String lineNumber = resultParts[3];
                        String combinedResults = resultParts[4];

                        // Split the combined results and process each one
                        String[] results = combinedResults.split("\\|");
                        for (String result : results) {
                            selfNode.writeToLogFilePublic(resultJobId, Arrays.asList(result), uniqueId, filename,
                                    lineNumber);
                        }
                    }
                    break;
                case "JOB2_START":
                    String[] startData = xmlDocument.getDocumentElement().getTextContent().split(";");
                    int newJobId = Integer.parseInt(startData[0]);
                    selfNode.jobId = newJobId;
                    // Deserialize and store the operator
                    Operators operator = Operators.deserialize(startData[1]);
                    selfNode.jobOperators.put(newJobId, operator);
                    System.out.println("Updated to Job " + selfNode.jobId + " (Operation 2)");
                    break;
                case "CHUNK_ASSIGNMENTS":
                    if (selfNode.getStageAssignment() == 2) {
                        String[] assignmentData = xmlDocument.getDocumentElement().getTextContent().split(";");
                        int assignedJobId = Integer.parseInt(assignmentData[0]);
                        String[] assignments = assignmentData[1].split("\\|");
                        for (String assignment : assignments) {
                            String[] chunkData = assignment.split(":");
                            String assignedChunkFile = chunkData[0];
                            String workerId = chunkData[1];
                            String key = assignedJobId + "_" + workerId;
                            selfNode.chunkAssignments.put(key, new Node.ChunkAssignment(assignedChunkFile, workerId));
                        }
                        System.out.println("Stage 2: Received chunk assignments: " + selfNode.chunkAssignments);
                    }
                    break;
            }
        }
    }

    public void startProtocol() {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(this::pingSelectedNodes, 0, 4, TimeUnit.SECONDS);
        executor.scheduleAtFixedRate(this::checkForFailures, 0, 4, TimeUnit.SECONDS);
        executor.scheduleAtFixedRate(this::checkReplication, 3, 10, TimeUnit.SECONDS);
        executor.scheduleAtFixedRate(this::checkForInvalidCache, 0, 10, TimeUnit.SECONDS);
        LOGGER.info("Protocol started for node: " + selfNode.id);
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: java FileSystem <introducer-ip> <introducer-port>");
            return;
        }

        introducerIp = args[0];
        introducerPort = Integer.parseInt(args[1]);

        int port = 6002;

        File localFileDir = new File("./data_storage/local_file/");
        if (localFileDir.exists()) {
            File[] files = localFileDir.listFiles((dir, name) -> name.startsWith("File_job_id_"));
            if (files != null) {
                for (File file : files) {
                    if (file.delete()) {
                        System.out.println("Cleaned up existing job file: " + file.getName());
                    }
                }
            }
        }

        Path path = Paths.get("data_storage/HyDFS/");
        try {
            // Create the directory if it doesn't exist
            if (Files.notExists(path)) {
                Files.createDirectory(path);
                System.out.println("Directory created successfully!");
            } else {
                // If the directory exists, clear its contents
                try (Stream<Path> paths = Files.list(path)) {
                    paths.forEach(file -> {
                        try {
                            Files.delete(file);
                            System.out.println("Deleted file: " + file);
                        } catch (IOException e) {
                            System.err.println("Failed to delete file: " + e.getMessage());
                        }
                    });
                }
                System.out.println("Directory contents cleared.");
            }

        } catch (IOException e) {
            System.err.println("Failed to create or clear directory: " + e.getMessage());
        }
        InetAddress localHost;
        try {
            localHost = InetAddress.getLocalHost();
            String ipAddress = localHost.getHostAddress();
            String nodeId = generateHashID(ipAddress.concat(Integer.toString(port)));
            System.out.println(nodeId + " " + ipAddress + " " + port);
            Node selfNode = new Node(nodeId, ipAddress, port);
            // Node ID = IP;Port;Timestamp
            RainStorm RainStorm = new RainStorm(selfNode);
            selfNode.rain_storm = RainStorm;
            Thread fileServerThread = new Thread(new FileServer(5002, selfNode.port));
            fileServerThread.start();
            RainStorm.joinIntroducer();
            RainStorm.startProtocol();
            LOGGER.info("RainStorm main method completed initialization");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public List<String> getMemberList() {
        return memberList;
    }

    public static Map<String, String> getMemberMap() {
        return memberMap;
    }
}
