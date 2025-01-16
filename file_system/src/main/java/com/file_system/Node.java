package com.file_system;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;

public class Node {
    public Map<String, ChunkAssignment> chunkAssignments = new HashMap<>();
    private Map<String, Integer> taskLinesProcessed = new ConcurrentHashMap<>();

    public static class TaskInfo {
        public String workerId;
        public long timestamp;

        public TaskInfo(String workerId, long timestamp) {
            this.workerId = workerId;
            this.timestamp = timestamp;
        }
    }

    public static class ChunkAssignment {
        public String chunkFileName;
        public String workerId;
        public String status;

        public ChunkAssignment(String chunkFileName, String workerId) {
            this.chunkFileName = chunkFileName;
            this.workerId = workerId;
            this.status = "PENDING";
        }
    }

    private static class TaskStatus {
        int linesProcessed;
        String status;
        boolean isComplete;

        TaskStatus() {
            this.linesProcessed = 0;
            this.status = "PENDING";
            this.isComplete = false;
        }
    }

    String id;
    boolean isAlive;
    long lastHeartbeat;
    String ipAddress;
    int port;
    DatagramSocket socket;
    DatagramSocket failureDetectionSocket;
    int incarnationNumber;
    boolean isLeader;
    String leader_id;
    RainStorm rain_storm;
    public int jobId = 0;
    private final String chunkDirectory = "data_storage/local_file/chunks/";
    public int stageAssignment; // 1 or 2
    private int numTasksPerNode;
    private Map<Integer, TaskStatus> taskStatuses = new HashMap<>(); // Track status of each task
    private Map<Integer, List<String>> stage2TaskAssignments = new HashMap<>(); // taskId -> list of worker IDs
    private List<ServerSocket> stage2Servers = new ArrayList<>();
    private volatile boolean isProcessingJob = false;
    public Map<Integer, Map<String, Boolean>> jobCompletionStatus = new HashMap<>(); // jobId -> (workerId -> completed)
    public Map<Integer, Operators> jobOperators = new HashMap<>();
    private final String outputDirectory = "data_storage/local_file/output/";
    private BufferedWriter currentJobWriter;
    private String[] columns = { "X", "Y", "OBJECTID", "Sign_Type", "Size_", "Supplement", "Sign_Post", "Year_Insta",
            "Category", "Notes", "MUTCD", "Ownership", "FACILITYID", "Schools", "Location_Adjusted", "Replacement_Zone",
            "Sign_Text", "Set_ID", "FieldVerifiedDate" };
    private Map<String, Integer> processedLines = new HashMap<>(); // Track lines processed by each worker
    private Map<Integer, Set<String>> processedTuples = new ConcurrentHashMap<>();
    private Map<String, Map<String, Long>> pendingTuples = new ConcurrentHashMap<>(); // tupleId -> (workerId ->
                                                                                      // lastTryTime)
    private ScheduledExecutorService retryScheduler = Executors.newScheduledThreadPool(1);
    private static final String LOG_DIRECTORY = "data_storage/local_file/logs/";
    private PrintWriter stageLogWriter;
    private Map<Integer, ScheduledExecutorService> jobRetrySchedulers = new ConcurrentHashMap<>();
    private volatile boolean isProcessing = false;
    private BufferedReader currentReader = null;
    private Map<Integer, Map<String, TupleInfo>> unacknowledgedTuples = new ConcurrentHashMap<>();
    private Map<Integer, ScheduledExecutorService> retrySchedulers = new ConcurrentHashMap<>(); // jobId -> scheduler
    private static final long RETRY_TIMEOUT = 5000; // 5 seconds
    private Boolean isScriptEnabled = false;
    private static Thread terminatorThread;

    private static class TupleInfo {
        String uniqueId;
        String chunkFileName;
        String lineNumber;
        String line;
        long sendTime;

        TupleInfo(String uniqueId, String chunkFileName, String lineNumber, String line) {
            this.uniqueId = uniqueId;
            this.chunkFileName = chunkFileName;
            this.lineNumber = lineNumber;
            this.line = line;
            this.sendTime = System.currentTimeMillis();
        }
    }

    private static final String LOG_FILE = "data_storage/local_file/logs/node_%s.log";
    private PrintWriter logger;

    Node(String id, String ipAddress, int port) {
        this.id = id;
        this.isAlive = true;
        this.lastHeartbeat = System.currentTimeMillis();
        this.ipAddress = ipAddress;
        this.port = port;
        this.incarnationNumber = 0;
        this.isLeader = false;

        // Initialize logger
        try {
            Files.createDirectories(Paths.get("data_storage/local_file/logs"));
            this.logger = new PrintWriter(new FileWriter(String.format(LOG_FILE, id), true), true);
            log("Node initialized with ID: " + id);
        } catch (IOException e) {
            System.err.println("Failed to create logger: " + e.getMessage());
        }

        try {
            this.socket = new DatagramSocket(this.port);
            log("Socket created on port: " + this.port);
        } catch (Exception e) {
            log("Failed to create sockets: " + e.getMessage(), "ERROR");
        }
    }

    private void log(String message) {
        log(message, "INFO");
    }

    private void log(String message, String level) {
        String timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
                .format(new java.util.Date());
        String logMessage = String.format("[%s] [%s] [Node-%s] %s", timestamp, level, id, message);

        if (logger != null) {
            logger.println(logMessage);
            logger.flush();
        }
        // Also print to console for debugging
        System.out.println(logMessage);
    }

    public void setLeader(String leader_id) {
        this.leader_id = leader_id;
        if (this.leader_id.equals(this.id)) {
            isLeader = true;
            System.out.println(id + " is elected as leader");
            try {
                File outputDir = new File(outputDirectory);
                Files.createDirectories(Paths.get(outputDirectory));
                if (outputDir.exists()) {
                    File[] files = outputDir.listFiles();
                    if (files != null) {
                        for (File file : files) {
                            if (file.delete()) {
                                System.out.println("Cleaned up output file: " + file.getName());
                            }
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println("Error cleaning output directory: " + e.getMessage());
            }
        }
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

    public void receivePing(String sender, Boolean isNotDropped) {
        if (isNotDropped) {
            // System.out.println("Ping received from " + sender);
            sendAcknowledgment(sender);
        }
    }

    public void sendPing(String target) {
        try {
            String pingMessage = createCustomXML("PING", this.id);
            byte[] buffer = pingMessage.getBytes();
            String[] targetDetails = target.split(";");
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length,
                    InetAddress.getByName(targetDetails[0]),
                    Integer.parseInt(targetDetails[1]));
            socket.send(packet);

            // System.out.println("Ping sent to " + target);
        } catch (IOException e) {
            System.err.println("Error sending ping: " + e.getMessage());
        }
    }

    public void sendAcknowledgment(String target) {
        try {
            String ackMessage = createCustomXML("ACK", this.id);
            byte[] buffer = ackMessage.getBytes();
            String[] targetDetails = target.split(";");
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length,
                    InetAddress.getByName(targetDetails[0]),
                    Integer.parseInt(targetDetails[1]));
            socket.send(packet);
            // System.out.println("ACK sent to " + target);
        } catch (IOException e) {
            System.err.println("Error sending acknowledgment: " + e.getMessage());
        }
    }

    public void incrementIncarnation() {
        incarnationNumber++;
    }

    public void processRainStormOperation(String operations) {
        Document xmlDocument = rain_storm.convertStringToXMLDocument(operations);
        System.out.println("Received RainStorm operation: " + operations);
        String content = xmlDocument.getDocumentElement().getTextContent();

        // Parse the content preserving quoted strings
        List<String> parts = parseOperationString(content);

        if (parts.size() < 6) {
            System.err.println("Invalid operation format. Expected: RAINSTORM operation1 operation2 filename numTasks true/false");
            return;
        }

        // Skip "RAINSTORM" and get the operations and parameters
        String operation1 = parts.get(1); // First operation after RAINSTORM
        String operation2 = parts.get(2); // Second operation
        String hyDFSFileName = parts.get(3); // Filename
        numTasksPerNode = Integer.parseInt(parts.get(4)); // Number of tasks
        this.isScriptEnabled = Boolean.parseBoolean(parts.get(5)); // Boolean for script that kills two worker nodes after 1.5s

        // Increment jobId before starting the job
        jobId++;

        if (isLeader) {
            System.out.println("Leader fetching file from HyDFS");
            rain_storm.getFromHyDFS(hyDFSFileName, "File_job_id_" + jobId + ".txt");
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        setupJobOperators(operation1, operation2);
        System.out.println("Operators setup complete. Current jobId: " + jobId);
        System.out.println("Available operators: " + jobOperators);

        try {
            Files.createDirectories(Paths.get(outputDirectory));
        } catch (IOException e) {
            System.err.println("Error creating output directory: " + e.getMessage());
            return;
        }

        System.out.println("Starting RainStorm operation with jobId: " + jobId);

        // Assign workers to stages
        assignWorkersToStages();

        // Set processing flag
        isProcessingJob = true;
        System.out.println("Set isProcessingJob to true for job " + jobId);

        // Cleanup any existing stage 2 servers before starting a new job
        if (stageAssignment == 2) {
            cleanupStage2Servers();
        }
        System.out.println("Stage 2 servers cleanup complete");

        // Now distribute the file if we're the leader
        if (isLeader) {
            System.out.println("Leader distributing file to Stage 1 workers");
            String filePath = "./data_storage/local_file/File_job_id_" + jobId + ".txt";
            try {
                System.out.println("Leader starting to distribute file to Stage 1 workers");
                this.splitAndDistributeToStage1(filePath);
            } catch (Exception e) {
                System.err.println("Error distributing file to Stage 1 workers:");
                e.printStackTrace();
            }
        }
        System.out.println("RainStorm operation setup complete");
    }

    private List<String> parseOperationString(String input) {
        List<String> tokens = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);

            if (c == '"') {
                inQuotes = !inQuotes;
                currentToken.append(c);
            } else if (c == ' ' && !inQuotes) {
                if (currentToken.length() > 0) {
                    tokens.add(currentToken.toString());
                    currentToken.setLength(0);
                }
            } else {
                currentToken.append(c);
            }
        }

        // Add the last token if there is one
        if (currentToken.length() > 0) {
            tokens.add(currentToken.toString());
        }

        return tokens;
    }

    private void setupJobOperators(String operation1, String operation2) {
        System.out.println("Setting up Job Operators");
        System.out.println("Operation 1: " + operation1);
        System.out.println("Operation 2: " + operation2);
        // Create operators
        Operators operator1 = createOperator(operation1, jobId);
        Operators operator2 = createOperator(operation2, jobId + 1);

        if (operator1 != null) {
            jobOperators.put(jobId, operator1);
            System.out.println("Job " + jobId + " operator: " + operator1.getOperationId());
        }
        if (operator2 != null) {
            jobOperators.put(jobId + 1, operator2);
            System.out.println("Job " + (jobId + 1) + " operator: " + operator2.getOperationId());
        }
    }

    private Operators createOperator(String operationString, int jobId) {
        System.out.println("operationString: " + operationString);
        String[] parts = operationString.split(":", 2);
        System.out.println("parts length: " + parts.length);
        String operationType = parts[0].toUpperCase();
        try {
            switch (operationType) {
                case "FILTER":
                    return new Operators("FILTER:" + parts[1],
                            Operators.createFilter(parts[1].trim()));
                case "COLUMN_FILTER":
                    String[] filterParts = parts[1].split(":", 2);
                    if (filterParts.length == 2) {
                        String columnName = filterParts[0].trim();
                        String targetValue = filterParts[1].trim();
                        // Handle quoted values like in FILTER
                        if (targetValue.startsWith("\"") && targetValue.endsWith("\"")) {
                            targetValue = targetValue.substring(1, targetValue.length() - 1);
                        }
                        // Find column index
                        int columnIndex = -1;
                        for (int i = 0; i < columns.length; i++) {
                            if (columns[i].trim().equals(columnName)) {
                                columnIndex = i;
                                break;
                            }
                        }
                        if (columnIndex != -1) {
                            return new Operators(operationString, String.valueOf(columnIndex), targetValue);
                        } else {
                            System.err.println("Column not found: " + columnName);
                            return null;
                        }
                    }
                    System.err.println("Invalid COLUMN_FILTER format. Expected COLUMN_FILTER:columnName:value");
                    return null;
                case "TRANSFORM":
                    if (parts[1].startsWith("select:")) {
                        // Handle select transform
                        String columnNames = parts[1].substring("select:".length());
                        System.out.println("Select transform columns: " + columnNames);

                        // Convert column names to indices
                        StringBuilder indices = new StringBuilder();
                        String[] columnList = columnNames.split(",");
                        for (int i = 0; i < columnList.length; i++) {
                            String columnName = columnList[i].trim();
                            int columnIndex = -1;
                            for (int j = 0; j < columns.length; j++) {
                                if (columns[j].trim().equals(columnName)) {
                                    columnIndex = j;
                                    break;
                                }
                            }
                            if (columnIndex != -1) {
                                if (indices.length() > 0) {
                                    indices.append(",");
                                }
                                indices.append(columnIndex);
                            } else {
                                System.err.println("Column not found: " + columnName);
                                return null;
                            }
                        }

                        String selectOperation = "TRANSFORM:select:" + indices.toString();
                        System.out.println("Created select operation: " + selectOperation);
                        return new Operators(selectOperation,
                                Operators.createTransform("select:" + indices.toString()));
                    } else {
                        return new Operators("TRANSFORM:" + parts[1],
                                Operators.createTransform(parts[1].trim()));
                    }
                case "AGGREGATE":
                    System.out.println("Creating AGGREGATE operator for running count");
                    return new Operators("AGGREGATE");
                default:
                    throw new IllegalArgumentException("Unknown operation type: " + operationType);
            }
        } catch (Exception e) {
            System.err.println("Error creating operator: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public void writeToLogFilePublic(int jobId, List<String> results, String uniqueId, String filename,
            String lineNumber) {
        writeToLogFile(jobId, results, uniqueId, filename, lineNumber);
    }

    public void closeCurrentJobWriter() {
        if (currentJobWriter != null) {
            try {
                currentJobWriter.close();
                currentJobWriter = null;
            } catch (IOException e) {
                System.err.println("Error closing job writer: " + e.getMessage());
            }
        }
    }

    private void assignWorkersToStages() {
        System.out.println("Assigning workers to stages");
        List<String> workers = rain_storm.memberList;
        List<String> availableWorkers = workers.stream()
                .filter(w -> !w.equals(leader_id))
                .collect(Collectors.toList());

        int totalWorkers = availableWorkers.size();
        int stage1Workers = totalWorkers / 2;
        int stage2Workers = totalWorkers - stage1Workers;

        // First, assign Stage 2 workers
        for (int i = stage1Workers; i < totalWorkers; i++) {
            String workerId = availableWorkers.get(i);
            int taskId = i - stage1Workers;
            stage2TaskAssignments.computeIfAbsent(taskId, k -> new ArrayList<>()).add(workerId);

            // Ensure we have the operator before sending stage assignment
            Operators operator = jobOperators.get(jobId);
            if (operator == null) {
                System.err.println("No operator found for job " + jobId + " in jobOperators map");
                System.err.println("Current jobOperators: " + jobOperators);
                continue;
            }
            System.out.println("Found operator for job " + jobId + ": " + operator.getOperationId());
            if(taskId < 2 ) {
                sendStageAssignment(workerId, 2, null, this.isScriptEnabled);
            } else {
                sendStageAssignment(workerId, 2, null, false);
            }
            System.out.println("Node " + workerId + " assigned to Stage 2");
        }

        // Then assign Stage 1 workers
        for (int i = 0; i < stage1Workers; i++) {
            String workerId = availableWorkers.get(i);
            Map<Integer, List<String>> stage2Assignments = new HashMap<>(stage2TaskAssignments);
            sendStageAssignment(workerId, 1, stage2Assignments, false);
            System.out.println("Node " + workerId + " assigned to Stage 1");
        }

        System.out.println("Stage assignments complete");
        System.out.println("Stage 2 assignments: " + stage2TaskAssignments);
    }

    private void sendStageAssignment(String workerId, int stage, Map<Integer, List<String>> stage2Assignments, Boolean scriptEnabled) {
        try {
            String[] nodeIpPort = RainStorm.memberMap.get(workerId).split(";");
            String workerIp = nodeIpPort[0];
            int workerPort = Integer.parseInt(nodeIpPort[1]);

            StringBuilder assignmentData = new StringBuilder(stage + ";" + numTasksPerNode);

            if (stage == 1 && stage2Assignments != null) {
                // Add stage 2 assignments to the message
                assignmentData.append(";").append(convertStage2AssignmentsToString(stage2Assignments));
            } else if (stage == 2) {
                // For Stage 2 workers, send both chunk assignments and operator
                assignmentData.append(";").append(convertChunkAssignmentsToString());

                // Add operator information
                Operators operator = jobOperators.get(jobId);
                if (operator != null) {
                    assignmentData.append(";")
                            .append(jobId)
                            .append(";")
                            .append(operator.serialize())
                            .append(";")
                            .append(scriptEnabled);
                    System.out.println("Stage 2 Assignment - JobId: " + jobId + ", Operator: " + operator + ", Script Enabled: " + scriptEnabled);
                    System.out.println("Assignment Data: " + Arrays.toString(assignmentData.toString().split(";")));
                } else {
                    System.err.println(
                            "Warning: No operator found for job " + jobId + " in jobOperators: " + jobOperators);
                    // Send a default operator to prevent null issues

                }
            }

            String assignmentMessage = createCustomXML("STAGE_ASSIGNMENT", assignmentData.toString());
            System.out.println("Sending stage assignment to " + workerId + ": " + assignmentMessage);

            byte[] buffer = assignmentMessage.getBytes();
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length,
                    InetAddress.getByName(workerIp), workerPort);
            socket.send(packet);
        } catch (IOException e) {
            System.err.println("Error sending stage assignment: " + e.getMessage());
        }
    }

    private String convertStage2AssignmentsToString(Map<Integer, List<String>> assignments) {
        return assignments.entrySet().stream()
                .map(entry -> entry.getKey() + ":" + String.join(",", entry.getValue()))
                .collect(Collectors.joining("|"));
    }

    private String convertChunkAssignmentsToString() {
        return chunkAssignments.entrySet().stream()
                .map(entry -> entry.getValue().chunkFileName + ":" + entry.getValue().workerId)
                .collect(Collectors.joining("|"));
    }

    public void splitAndDistributeToStage1(String filePath) throws IOException {
        Files.createDirectories(Paths.get(chunkDirectory));

        // Get workers already assigned to stage 1
        List<String> stage1Workers = rain_storm.getMemberList().stream()
                .filter(w -> !w.equals(leader_id) && stage2TaskAssignments.values().stream()
                        .flatMap(List::stream)
                        .noneMatch(s2w -> s2w.equals(w)))
                .collect(Collectors.toList());

        System.out.println("Found " + stage1Workers.size() + " Stage 1 workers");
        if (stage1Workers.isEmpty()) {
            System.out.println("Error: No Stage 1 workers available.");
            return;
        }

        // Create chunks and assignments first
        Map<String, ChunkAssignment> newChunkAssignments = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            int totalLines = countLines(filePath);
            int numWorkers = stage1Workers.size();
            int baseLines = totalLines / numWorkers;
            int extraLines = totalLines % numWorkers;
            int currentLine = 0;

            for (int i = 0; i < stage1Workers.size(); i++) {
                int linesToRead = baseLines + (i < extraLines ? 1 : 0);
                String chunkFileName = this.jobId + "_stage1_chunk_" + (i + 1) + ".txt";
                String chunkFilePath = chunkDirectory + chunkFileName;

                createChunkFile(reader, chunkFilePath, linesToRead);
                currentLine += linesToRead;

                String workerId = stage1Workers.get(i);
                String assignmentKey = jobId + "_" + workerId;
                newChunkAssignments.put(assignmentKey, new ChunkAssignment(chunkFileName, workerId));
            }
        }

        // Broadcast chunk assignments to all Stage 2 workers
        String chunkAssignmentsMsg = createCustomXML("CHUNK_ASSIGNMENTS",
                jobId + ";" + convertChunkAssignmentsToString(newChunkAssignments));

        for (List<String> workers : stage2TaskAssignments.values()) {
            for (String workerId : workers) {
                try {
                    String[] nodeIpPort = RainStorm.getMemberMap().get(workerId).split(";");
                    DatagramPacket packet = new DatagramPacket(
                            chunkAssignmentsMsg.getBytes(),
                            chunkAssignmentsMsg.getBytes().length,
                            InetAddress.getByName(nodeIpPort[0]),
                            Integer.parseInt(nodeIpPort[1]));
                    socket.send(packet);
                    System.out.println("Sent chunk assignments to Stage 2 worker: " + workerId);
                } catch (IOException e) {
                    System.err.println("Error sending chunk assignments to " + workerId + ": " + e.getMessage());
                }
            }
        }

        // Now distribute chunks to Stage 1 workers
        for (Map.Entry<String, ChunkAssignment> entry : newChunkAssignments.entrySet()) {
            String workerId = entry.getValue().workerId;
            String chunkFileName = entry.getValue().chunkFileName;
            String chunkFilePath = chunkDirectory + chunkFileName;
            sendChunkToStage1Worker(chunkFileName, chunkFilePath, workerId);
        }

        // Update local chunk assignments
        chunkAssignments.putAll(newChunkAssignments);
    }

    private String convertChunkAssignmentsToString(Map<String, ChunkAssignment> assignments) {
        return assignments.entrySet().stream()
                .map(entry -> entry.getValue().chunkFileName + ":" + entry.getValue().workerId)
                .collect(Collectors.joining("|"));
    }

    private void sendChunkToStage1Worker(String chunkFileName, String chunkFilePath, String workerId) {
        try {
            String[] nodeIpPort = RainStorm.getMemberMap().get(workerId).split(";");
            String workerIp = nodeIpPort[0];
            int workerFileServerPort = 5002;

            try (Socket socket = new Socket(workerIp, workerFileServerPort);
                    DataOutputStream dataOutputStream = new DataOutputStream(
                            new BufferedOutputStream(socket.getOutputStream()))) {

                File chunkFile = new File(chunkFilePath);
                long fileSize = chunkFile.length();

                // Send metadata according to FileServer's expected format
                dataOutputStream.writeUTF("JOB_ID:" + jobId); // replication_status
                dataOutputStream.writeUTF(chunkFileName); // fileName
                dataOutputStream.writeLong(fileSize); // fileSize
                dataOutputStream.flush(); // Flush after metadata

                try (FileInputStream fileInputStream = new FileInputStream(chunkFile);
                        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream, 8192)) {

                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    long totalBytesWritten = 0;
                    while ((bytesRead = bufferedInputStream.read(buffer)) != -1) {
                        dataOutputStream.write(buffer, 0, bytesRead);
                        totalBytesWritten += bytesRead;
                        if (totalBytesWritten % (8192 * 10) == 0) { // Flush periodically
                            dataOutputStream.flush();
                        }
                    }
                    dataOutputStream.flush(); // Final flush
                }

                updateChunkStatus(jobId, workerId, "PROCESSING");
                System.out.println("Chunk " + chunkFileName + " sent to stage 1 worker " + workerId);
            }
        } catch (IOException e) {
            System.err.println("Error sending chunk to stage 1 worker: " + e.getMessage());
            updateChunkStatus(jobId, workerId, "FAILED");
        }
    }

    // Method to handle received chunks in Stage 1 workers
    public void handleStage1Chunk(String chunkFileName, InputStream inputStream) {
        if (stageAssignment != 1) {
            return;
        }

        try {
            String localChunkPath = chunkDirectory + chunkFileName;
            Files.createDirectories(Paths.get(chunkDirectory));

            // Save the chunk locally
            try (FileOutputStream fos = new FileOutputStream(localChunkPath);
                    BufferedOutputStream bos = new BufferedOutputStream(fos)) {
                // Read and write in chunks to avoid memory issues
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    bos.write(buffer, 0, bytesRead);
                }
                bos.flush();
            }

            // Initialize line flags
            processChunkTask(chunkFileName, localChunkPath);

        } catch (IOException e) {
            System.err.println("Error handling stage 1 chunk: " + e.getMessage());
        }
    }

    private int countLines(String filePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            int lines = 0;
            while (reader.readLine() != null)
                lines++;
            return lines;
        }
    }

    private void processChunkTask(String chunkFileName, String chunkPath) {
        String jobIdStr = chunkFileName.split("_")[0];
        int jobId = Integer.parseInt(jobIdStr);

        try (BufferedReader reader = new BufferedReader(new FileReader(chunkPath))) {
            String line;
            int lineNumber = 0;
            String uniqueId = UUID.randomUUID().toString();

            while ((line = reader.readLine()) != null && isProcessing) {
                // Process each line
                String processedData = processLine(chunkFileName + ":" + lineNumber, line, uniqueId);

                // Forward to Stage 2
                forwardToStage2(processedData);

                lineNumber++;
            }

            // Update task status
            updateChunkStatus(jobId, id, "COMPLETED");
            System.out.println("Completed processing chunk: " + chunkFileName);

        } catch (IOException e) {
            System.err.println("Error processing chunk: " + e.getMessage());
            updateChunkStatus(jobId, id, "FAILED");
        }
    }

    private void cleanupChunkFile(String chunkPath) {
        try {
            File chunkFile = new File(chunkPath);
            if (chunkFile.exists() && chunkFile.delete()) {
                System.out.println("Cleaned up chunk file: " + chunkPath);
            }
        } catch (Exception e) {
            System.err.println("Error cleaning up chunk file: " + e.getMessage());
        }
    }

    private void cleanupJobFiles(int jobId) {
        if (isLeader) {
            // Cleanup job file from leader
            String jobFile = "./data_storage/local_file/File_job_id_" + jobId + ".txt";
            try {
                File file = new File(jobFile);
                if (file.exists() && file.delete()) {
                    System.out.println("Cleaned up job file: " + jobFile);
                }
            } catch (Exception e) {
                System.err.println("Error cleaning up job file: " + e.getMessage());
            }
        }
    }

    public void handleTaskStatus(String statusMessage) {
        if (!isLeader)
            return;

        String[] parts = statusMessage.split(";");
        int reportedJobId = Integer.parseInt(parts[0]);
        int reportedTaskId = Integer.parseInt(parts[1]);
        String workerId = parts[2];
        int linesProcessed = Integer.parseInt(parts[3]);
        String status = parts[4];

        System.out.println(String.format(
                "Task status update - Job: %d, Task: %d, Worker: %s, Lines: %d, Status: %s",
                reportedJobId, reportedTaskId, workerId, linesProcessed, status));

        updateTaskStatus(reportedJobId, workerId, reportedTaskId, status, linesProcessed);

        // Update job completion status with proper Integer key
        jobCompletionStatus.computeIfAbsent(reportedJobId, k -> new HashMap<>())
                .put(workerId, "COMPLETED".equals(status));

        // Check if all workers have completed all their tasks
        if (isJobComplete(reportedJobId)) {
            System.out.println("All tasks complete for job " + reportedJobId);

            // If this is operation 1, start operation 2
            if (reportedJobId % 2 == 1) {
                if (currentJobWriter != null) {
                    try {
                        currentJobWriter.close();
                        currentJobWriter = null;
                    } catch (IOException e) {
                        System.err.println("Error closing writer: " + e.getMessage());
                    }
                }
                System.out.println("Operation 1 complete, starting Operation 2");
                startOperation2();
            }

            cleanupJobFiles(reportedJobId);
            jobCompletionStatus.remove(reportedJobId);
        }
    }

    private void updateTaskStatus(int jobId, String workerId, int taskId, String status, int linesProcessed) {
        String key = String.format("%d_%s_%d", jobId, workerId, taskId);
        TaskStatus taskStatus = taskStatuses.computeIfAbsent(taskId, k -> new TaskStatus());
        taskStatus.status = status;
        taskStatus.linesProcessed = linesProcessed;
        taskStatus.isComplete = "COMPLETED".equals(status);

        // Update job completion status
        jobCompletionStatus.computeIfAbsent(jobId, k -> new ConcurrentHashMap<>())
                .put(workerId, "COMPLETED".equals(status));
    }

    private boolean isJobComplete(int jobId) {
        Map<String, Boolean> workerStatus = jobCompletionStatus.get(jobId); // Use Integer directly
        if (workerStatus == null)
            return false;

        // Get all workers assigned to this job
        List<String> jobWorkers = chunkAssignments.entrySet().stream()
                .filter(e -> e.getKey().startsWith(jobId + "_"))
                .map(e -> e.getValue().workerId)
                .collect(Collectors.toList());

        // Check if all workers have reported completion
        return jobWorkers.size() == workerStatus.size() &&
                workerStatus.values().stream().allMatch(status -> status);
    }

    public Map<String, Map<Integer, TaskStatus>> getJobTaskStatuses(int jobId) {
        Map<String, Map<Integer, TaskStatus>> statusMap = new HashMap<>();
        for (Map.Entry<String, ChunkAssignment> entry : chunkAssignments.entrySet()) {
            if (entry.getKey().startsWith(jobId + "_")) {
                String workerId = entry.getValue().workerId;
                statusMap.computeIfAbsent(workerId, k -> new HashMap<>())
                        .putAll(taskStatuses);
            }
        }
        return statusMap;
    }

    private String processLine(String fileLineNumber, String line, String uniqueId) {
        // Create tuple in format <uniqueId:filename,linenumber,line>
        String[] parts = fileLineNumber.split(":");
        String filename = parts[0];
        String lineNumber = parts[1];
        return String.format("%s:%s,%s,%s", uniqueId, filename, lineNumber, line);
    }

    private void forwardToStage2(String processedData) {
        log("Forwarding data to Stage 2: " + processedData);
        // Parse the processed data to get components for tracking
        String[] parts = processedData.split(":", 2);
        String uniqueId = parts[0];
        String[] dataParts = parts[1].split(",", 3);
        String chunkFileName = dataParts[0];
        String lineNumber = dataParts[1];
        String line = dataParts[2];

        // Store the full tuple information
        int jobId = Integer.parseInt(chunkFileName.split("_")[0]);
        String tupleId = uniqueId + ":" + chunkFileName + ":" + lineNumber;
        unacknowledgedTuples.computeIfAbsent(jobId, k -> new ConcurrentHashMap<>())
                .put(tupleId, new TupleInfo(uniqueId, chunkFileName, lineNumber, line));

        // Add sender ID to the processed data
        String tupleWithSender = this.id + ";" + processedData;

        // Use line number for distribution to ensure even spread across Stage 2 nodes
        int lineNum = Integer.parseInt(lineNumber);
        int totalStage2Workers = stage2TaskAssignments.values().stream()
                .mapToInt(List::size)
                .sum();

        if (totalStage2Workers == 0) {
            log("No Stage 2 workers available", "ERROR");
            return;
        }

        // Simple round-robin distribution based on line number
        int workerIndex = lineNum % totalStage2Workers;

        // Find the target worker
        String targetWorkerId = null;
        int currentIndex = 0;
        for (List<String> workers : stage2TaskAssignments.values()) {
            for (String workerId : workers) {
                if (currentIndex == workerIndex) {
                    targetWorkerId = workerId;
                    break;
                }
                currentIndex++;
            }
            if (targetWorkerId != null)
                break;
        }

        if (targetWorkerId != null) {
            try {
                String[] nodeIpPort = RainStorm.getMemberMap().get(targetWorkerId).split(";");
                String workerIp = nodeIpPort[0];
                int taskPort = 7000;

                try (Socket socket = new Socket(workerIp, taskPort)) {
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    writer.println(tupleWithSender);
                    startRetryMechanism(jobId);
                } catch (IOException e) {
                    System.err.println("Error sending to Stage 2 worker: " + e.getMessage());
                }
            } catch (Exception e) {
                System.err.println("Error sending to Stage 2 worker: " + e.getMessage());
            }
        }
        log("Successfully forwarded to Stage 2 worker: " + targetWorkerId);
    }

    private void retryTuple(int jobId, String tupleId) {
        log("Retrying tuple for job " + jobId + ": " + tupleId);
        Map<String, TupleInfo> jobTuples = unacknowledgedTuples.get(jobId);
        if (jobTuples != null) {
            TupleInfo tupleInfo = jobTuples.get(tupleId);
            if (tupleInfo != null) {
                // Reconstruct the tuple using stored information
                String processedData = String.format("%s:%s,%s,%s",
                        tupleInfo.uniqueId, tupleInfo.chunkFileName, tupleInfo.lineNumber, tupleInfo.line);
                forwardToStage2(processedData);
            }
        }
    }

    private void startAckServers() {
        if (stageAssignment != 1)
            return;

        final int ackPort = 6500;
        try {
            ServerSocket ackServer = new ServerSocket(ackPort);
            System.out.println("Started ACK server on port " + ackPort);

            // Single thread to handle acknowledgments
            new Thread(() -> {
                while (!ackServer.isClosed()) {
                    try (Socket client = ackServer.accept();
                            BufferedReader reader = new BufferedReader(
                                    new InputStreamReader(client.getInputStream()))) {

                        String ackMessage = reader.readLine();
                        if (ackMessage != null) {
                            Document xmlDoc = rain_storm.convertStringToXMLDocument(ackMessage);
                            if (xmlDoc != null && "STAGE2_ACK".equals(xmlDoc.getDocumentElement().getNodeName())) {
                                String trackingKey = xmlDoc.getDocumentElement().getTextContent();
                                removeAcknowledgedData(trackingKey);
                            }
                        }
                    } catch (IOException e) {
                        if (!ackServer.isClosed()) {
                            System.err.println("Error in ACK server: " + e.getMessage());
                        }
                    }
                }
            }).start();
        } catch (IOException e) {
            System.err.println("Failed to start ACK server on port 6500: " + e.getMessage());
        }
    }

    private synchronized void removeAcknowledgedData(String trackingKey) {
        log("Removing acknowledged data: " + trackingKey);
        String[] parts = trackingKey.split(":");
        String chunkFileName = parts[1];
        int jobId = Integer.parseInt(chunkFileName.split("_")[0]);

        Map<String, TupleInfo> jobTuples = unacknowledgedTuples.get(jobId);
        if (jobTuples != null) {
            jobTuples.remove(trackingKey);
            if (jobTuples.isEmpty()) {
                unacknowledgedTuples.remove(jobId);
            }
        }
    }

    public void updateStage2AssignmentOnFailure(String failedNodeId) {
        System.out.println("Inside update stage 2 assignment on failure: " + this.stageAssignment);
        if (this.stageAssignment == 1) {
            // Update stage2TaskAssignments
            Set<Integer> keys = stage2TaskAssignments.keySet();
            List<Integer> keysToRemove = new ArrayList<>();

            for (Integer key : keys) {
                if (stage2TaskAssignments.get(key).contains(failedNodeId)) {
                    keysToRemove.add(key);
                }
            }

            // Remove the keys after iteration
            for (Integer key : keysToRemove) {
                stage2TaskAssignments.remove(key);
            }
            System.out.println("After removing failed node: " + stage2TaskAssignments);

            // Redistribute unacknowledged tuples that were sent to the failed node
            for (Map.Entry<Integer, Map<String, TupleInfo>> jobEntry : unacknowledgedTuples.entrySet()) {
                int jobId = jobEntry.getKey();
                Map<String, TupleInfo> jobTuples = jobEntry.getValue();

                List<String> remainingWorkers = stage2TaskAssignments.values().stream()
                        .flatMap(List::stream)
                        .collect(Collectors.toList());

                if (!remainingWorkers.isEmpty()) {
                    Random rand = new Random();
                    for (String tupleId : new ArrayList<>(jobTuples.keySet())) {
                        String randomWorkerId = remainingWorkers.get(rand.nextInt(remainingWorkers.size()));
                        retryTuple(jobId, tupleId);
                    }
                }
            }
        }
    }

    public void shutdown() {
        log("Shutting down node " + id);
        // Shutdown all job schedulers
        jobRetrySchedulers.values().forEach(scheduler -> {
            if (!scheduler.isShutdown()) {
                scheduler.shutdown();
                try {
                    scheduler.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    scheduler.shutdownNow();
                }
            }
        });
        jobRetrySchedulers.clear();
        if (logger != null) {
            logger.close();
        }
    }

    private void startOperation2() {
        if (!isLeader)
            return;

        // Print Operation 1 completion stats
        System.out.println("\nOperation 1 Completed!");
        System.out.println("Lines processed by each Stage 1 worker:");
        for (Map.Entry<String, ChunkAssignment> entry : chunkAssignments.entrySet()) {
            if (entry.getKey().startsWith(jobId + "_")) {
                String workerId = entry.getValue().workerId;
                int linesProcessed = taskLinesProcessed.getOrDefault(workerId, 0);
                System.out.println("Worker " + workerId + ": " + linesProcessed + " lines");
            }
        }
        System.out.println();

        System.out.println("Starting Operation 2...");
        String op1ResultsFile = outputDirectory + "job" + jobId + "_results.log";
        String op2InputFile = "./data_storage/local_file/File_job_id_" + (jobId + 1) + ".txt";
        try {
            File resultsFile = new File(op1ResultsFile);
            if (!resultsFile.exists() || resultsFile.length() == 0) {
                System.err.println("No results from Operation 1 to process");
                return;
            }

            Files.copy(Paths.get(op1ResultsFile), Paths.get(op2InputFile),
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            System.out.println("Using Operation 1 results as input for Operation 2");

            jobId++;
            System.out.println("Job ID: " + jobOperators);
            Operators operator2 = jobOperators.get(jobId);
            System.out.println("Operator 2: " + operator2.getOperationId());
            String assignmentMessage = createCustomXML("JOB2_START",
                    String.format("%d;%s", jobId, operator2.serialize()));

            for (String workerId : rain_storm.getMemberList()) {
                if (!workerId.equals(id) && !workerId.equals(leader_id)) {
                    String[] nodeIpPort = RainStorm.memberMap.get(workerId).split(";");
                    DatagramPacket packet = new DatagramPacket(
                            assignmentMessage.getBytes(),
                            assignmentMessage.getBytes().length,
                            InetAddress.getByName(nodeIpPort[0]),
                            Integer.parseInt(nodeIpPort[1]));
                    socket.send(packet);
                }
            }
            isProcessingJob = true;
            taskLinesProcessed.clear();
            splitAndDistributeToStage1(op2InputFile);
        } catch (IOException e) {
            System.err.println("Error starting operation 2: " + e.getMessage());
        }
    }

    private void writeToLogFile(int jobId, List<String> results, String uniqueId, String workerId, String lineNumber) {
        String resultLogFile = outputDirectory + "job" + jobId + "_results.log";
        String tupleLogFile = outputDirectory + "job" + jobId + "_tuples.log";

        try {
            synchronized (this) {
                // Write results
                try (BufferedWriter resultWriter = new BufferedWriter(new FileWriter(resultLogFile, true));
                        BufferedWriter tupleWriter = new BufferedWriter(new FileWriter(tupleLogFile, true))) {
                    for (String result : results) {
                        resultWriter.write(result + "\n");
                        resultWriter.flush();
                        tupleWriter.write(String.format("%s,%s,%s,%s%n", uniqueId, workerId, lineNumber, result));
                        tupleWriter.flush();

                        // Print to leader's console
                        if (isLeader) {
                            log("Result from worker " + workerId + ": " + result);
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error writing to log files: " + e.getMessage());
        }
    }

    private void cleanupStage2Servers() {
        for (ServerSocket server : stage2Servers) {
            try {
                if (!server.isClosed()) {
                    server.close();
                }
            } catch (IOException e) {
                System.err.println("Error closing Stage 2 server: " + e.getMessage());
            }
        }
        stage2Servers.clear();
    }

    private void createChunkFile(BufferedReader reader, String chunkFilePath, int linesToRead)
            throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(chunkFilePath))) {
            String line;
            int linesWritten = 0;
            while (linesWritten < linesToRead && (line = reader.readLine()) != null) {
                writer.write(line);
                writer.newLine();
                linesWritten++;
            }
        }
    }

    private void updateChunkStatus(int jobId, String workerId, String status) {
        String key = jobId + "_" + workerId;
        ChunkAssignment assignment = chunkAssignments.get(key);
        if (assignment != null) {
            assignment.status = status;
            System.out.println("Updated chunk status for worker " + workerId + " to " + status);
        }
    }

    public void handleChunkFileReceived(String chunkFileName) {
        if (stageAssignment == 1) {
            System.out.println("Stage 1 worker received chunk: " + chunkFileName);
            try {
                Thread.sleep(100); // Wait briefly for file to be fully written
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            String chunkPath = "./data_storage/job_chunks/" + chunkFileName;
            if (!new File(chunkPath).exists()) {
                System.err.println("Chunk file not found: " + chunkPath);
                return;
            }
            System.out.println("Found chunk file, starting processing");
            try {
                startChunkProcessing(chunkFileName);
            } catch (Exception e) {
                System.err.println("Chunk processing interrupted: " + e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }

    public void handleStageAssignment(String assignmentData) {
        String[] parts = assignmentData.split(";");
        stageAssignment = Integer.parseInt(parts[0]);
        numTasksPerNode = Integer.parseInt(parts[1]);
        if (parts.length == 6) {
            this.isScriptEnabled = Boolean.parseBoolean(parts[5]);
        }
        System.out.println("assigned data received: " + assignmentData);

        isProcessingJob = true;
        System.out.println("Set isProcessingJob to true");

        if (stageAssignment == 2) {
            startStage2Servers();
            if (parts.length > 4) {
                try {
                    int assignedJobId = Integer.parseInt(parts[3]);
                    this.jobId = assignedJobId;
                    String operatorData = parts[4];
                    System.out.println("Received operator data: " + operatorData);

                    // Parse the operator data - it should be just the filter condition
                    String filterCondition = operatorData;
                    if (filterCondition.contains("|")) {
                        // Take just the first part if there are multiple parts
                        filterCondition = filterCondition.split("\\|")[0];
                    }

                    // Remove FILTER: prefix if present
                    if (filterCondition.startsWith("FILTER:")) {
                        filterCondition = filterCondition.substring("FILTER:".length());
                    }

                    // Create a new operator with the filter condition
                    Operators operator = new Operators("FILTER:" + filterCondition,
                            Operators.createFilter(filterCondition));
                    jobOperators.put(assignedJobId, operator);
                    System.out.println(
                            "Stage 2: Created operator for job " + assignedJobId + ": " + operator.getOperationId());
                } catch (Exception e) {
                    System.err.println("Error handling Stage 2 assignment: " + e.getMessage());
                    e.printStackTrace();
                }
            } else {
                System.err.println("Missing operator information in stage assignment. Parts length: " + parts.length);
                System.err.println("Assignment data: " + assignmentData);
            }
        } else if (stageAssignment == 1) {
            startAckServers();
            startRetryMechanism(jobId);

            if (parts.length > 2 && !parts[2].isEmpty()) {
                try {
                    Map<Integer, List<String>> stage2Assignments = new HashMap<>();
                    String[] assignments = parts[2].split("\\|");
                    for (String assignment : assignments) {
                        String[] taskData = assignment.split(":");
                        int taskId = Integer.parseInt(taskData[0]);
                        List<String> workers = Arrays.asList(taskData[1].split(","));
                        stage2Assignments.put(taskId, workers);
                    }
                    setStage2TaskAssignments(stage2Assignments);
                    System.out.println("Stage 1: Received Stage 2 assignments: " + stage2Assignments);
                } catch (Exception e) {
                    System.err.println("Error parsing Stage 2 assignments: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    private void killScript() {

        if (terminatorThread != null && terminatorThread.isAlive()) {
           // System.out.println("Terminator thread is already running.");
           return; // Do not start a new thread
       }

       terminatorThread = new Thread(() -> {
           try {
               // System.out.println("Terminator thread started. Waiting 1.5 seconds...");
               Thread.sleep(1500);

               // Get the current process PID
               long pid = ProcessHandle.current().pid();
               System.out.println("Sending Ctrl+C to the entire process PID: " + pid);

               // Send SIGINT (Ctrl+C) to the entire process
               new ProcessBuilder("kill", "-SIGINT", String.valueOf(pid)).start();
               // System.out.println("Ctrl+C sent. This line may not be reached if the process exits.");
           } catch (InterruptedException e) {
               System.err.println("Sleep interrupted: " + e.getMessage());
           } catch (IOException e) {
               System.err.println("Error sending signal: " + e.getMessage());
           }
       });
       terminatorThread.start();
   }


    public void startChunkProcessing(String chunkFileName) {
        if (!isProcessingJob) {
            System.out.println("Not processing job, skipping chunk processing");
            return;
        }

        String chunkPath = "./data_storage/job_chunks/" + chunkFileName;
        try {
            // Wait for file to be completely written
            Thread.sleep(1000);

            isProcessing = true;
            processChunkTask(chunkFileName, chunkPath);

            // Wait for acknowledgments after processing is complete
            waitForAcknowledgments(chunkFileName);

            int jobId = Integer.parseInt(chunkFileName.split("_")[0]);
            System.out.println("Processing completed for job " + jobId);
            reportCompletionToLeader(jobId);

        } catch (Exception e) {
            System.err.println("Error in chunk processing: " + e.getMessage());
            e.printStackTrace();
        } finally {
            isProcessing = false;
        }
    }

    private void waitForAcknowledgments(String chunkFileName) {
        log("Waiting for acknowledgments for chunk: " + chunkFileName);
        int jobId = Integer.parseInt(chunkFileName.split("_")[0]);
        Map<String, TupleInfo> jobTuples = unacknowledgedTuples.get(jobId);

        // Wait until all pending tuples for this job are acknowledged
        while (jobTuples != null && !jobTuples.isEmpty()) {
            try {
                Thread.sleep(100);
                System.out.println("Job " + jobId + " waiting for " + jobTuples.size() + " tuples to be acknowledged");
                log("Still waiting for " + jobTuples.size() + " acknowledgments");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        // Shutdown retry scheduler for this job
        ScheduledExecutorService scheduler = retrySchedulers.remove(jobId);
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            System.out.println("Shut down retry scheduler for job " + jobId);
        }

        // Remove job's unacknowledged tuples
        unacknowledgedTuples.remove(jobId);
        log("All acknowledgments received for chunk: " + chunkFileName);
    }

    private void reportCompletionToLeader(int jobId) {
        try {
            String statusMessage = createCustomXML("TASK_STATUS",
                    String.format("%d;%d;%s;%d;%s",
                            jobId, 0, id, 0, "COMPLETED"));

            String[] leaderDetails = RainStorm.getMemberMap().get(leader_id).split(";");
            DatagramPacket packet = new DatagramPacket(
                    statusMessage.getBytes(),
                    statusMessage.getBytes().length,
                    InetAddress.getByName(leaderDetails[0]),
                    Integer.parseInt(leaderDetails[1]));
            socket.send(packet);

            System.out.println("Worker " + id + " reported completion to leader for job " + jobId);
        } catch (IOException e) {
            System.err.println("Error reporting completion to leader: " + e.getMessage());
        }
    }

    public int getStageAssignment() {
        return stageAssignment;
    }

    public void setStage2TaskAssignments(Map<Integer, List<String>> assignments) {
        this.stage2TaskAssignments = assignments;
        System.out.println("Updated Stage 2 task assignments: " + assignments);
    }

    private void startStage2Servers() {
        final int taskPort = 7000;
        try {
            ServerSocket server = new ServerSocket(taskPort);
            stage2Servers.add(server);

            new Thread(() -> {
                while (!server.isClosed()) {
                    try (Socket client = server.accept();
                            BufferedReader reader = new BufferedReader(
                                    new InputStreamReader(client.getInputStream()))) {
                        String data = reader.readLine();
                        if (data != null) {
                            processStage2Data(0, data); // Single task ID
                            if (isScriptEnabled) {
                                killScript();
                            }
                        }
                    } catch (IOException e) {
                        if (!server.isClosed()) {
                            System.err.println("Error in Stage 2 server: " + e.getMessage());
                        }
                    }
                }
            }).start();

            System.out.println("Stage 2 server is ready to receive data on port " + taskPort);
        } catch (IOException e) {
            System.err.println("Error starting Stage 2 server: " + e.getMessage());
        }
    }

    private void processStage2Data(int taskId, String tupleData) {
        if (!isProcessingJob) {
            log("Skipping Stage 2 processing - not in processing state");
            return;
        }
        log("Processing Stage 2 data: " + tupleData);
        // Extract sender ID from tuple
        String[] senderAndData = tupleData.split(";", 2);
        String senderId = senderAndData[0];
        String actualTupleData = senderAndData[1];

        // Process the data
        String[] parts = actualTupleData.split(":", 2);
        String uniqueId = parts[0];
        String[] dataParts = parts[1].split(",", 3);
        String chunkFileName = dataParts[0];
        String lineNumber = dataParts[1];
        String line = dataParts[2];

        // Process with operator
        int jobId = Integer.parseInt(chunkFileName.split("_")[0]);
        Operators operator = jobOperators.get(jobId);
        if (operator != null) {
            List<String> results = operator.process(line);
            if (!results.isEmpty()) {
                if (isLeader) {
                    writeToLogFile(jobId, results, uniqueId, senderId, lineNumber);
                } else {
                    streamToLeader(results, uniqueId, senderId, lineNumber, String.valueOf(jobId));
                }
            }

            // Send acknowledgment directly to sender
            sendAcknowledgment(senderId, uniqueId + ":" + chunkFileName + ":" + lineNumber);
        }
    }

    private void streamToLeader(List<String> results, String uniqueId, String workerId, String lineNumber,
            String jobId) {
        try {
            String[] leaderIpPort = RainStorm.getMemberMap().get(leader_id).split(";");
            String leaderIp = leaderIpPort[0];
            int leaderPort = Integer.parseInt(leaderIpPort[1]);

            // Stream each result individually
            for (String result : results) {
                String resultMessage = createCustomXML("STAGE2_RESULT",
                        String.format("%s;%s;%s;%s;%s", jobId, uniqueId, workerId, lineNumber, result));

                byte[] buffer = resultMessage.getBytes();
                DatagramPacket packet = new DatagramPacket(
                        buffer, buffer.length,
                        InetAddress.getByName(leaderIp), leaderPort);
                socket.send(packet);
                System.out.println("Streamed result to leader: " + result);
            }
        } catch (IOException e) {
            System.err.println("Error streaming to leader: " + e.getMessage());
        }
    }

    // Simplified acknowledgment sending
    private void sendAcknowledgment(String senderId, String tupleId) {
        log("Sending acknowledgment to " + senderId + " for tuple: " + tupleId);
        try {
            String[] nodeIpPort = RainStorm.getMemberMap().get(senderId).split(";");
            String senderIp = nodeIpPort[0];
            int ackPort = 6500;

            try (Socket socket = new Socket(senderIp, ackPort)) {
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                // Create proper XML acknowledgment
                String ackXml = createCustomXML("STAGE2_ACK", tupleId);
                writer.println(ackXml);
            }
        } catch (IOException e) {
            System.err.println("Error sending acknowledgment: " + e.getMessage());
        }
    }

    // Simple retry mechanism
    private void startRetryMechanism(int jobId) {
        if (!retrySchedulers.containsKey(jobId)) {
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            retrySchedulers.put(jobId, scheduler);

            scheduler.scheduleAtFixedRate(() -> {
                Map<String, TupleInfo> jobTuples = unacknowledgedTuples.get(jobId);
                if (jobTuples != null) {
                    long now = System.currentTimeMillis();
                    jobTuples.forEach((tupleId, tupleInfo) -> {
                        if (now - tupleInfo.sendTime > RETRY_TIMEOUT) {
                            retryTuple(jobId, tupleId);
                        }
                    });
                }
            }, RETRY_TIMEOUT, RETRY_TIMEOUT, TimeUnit.MILLISECONDS);
        }
    }
}
