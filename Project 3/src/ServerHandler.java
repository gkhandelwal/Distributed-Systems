import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by sarda014 on 4/15/16.
 */
public class ServerHandler implements ServerService.Iface{

    Server pServer = null;
    private int NO_THREADS_PER_NODE = 3;
    //private int REPLICATION_FACTOR = 3;

    ReentrantLock computeNodeListLock = new ReentrantLock();
    ReentrantLock taskListLock = new ReentrantLock();
    ReentrantLock pendingTaskListLock = new ReentrantLock();
    ReentrantLock nodeTasksMapLock = new ReentrantLock();
    ReentrantLock nodeTasksFailedMapLock = new ReentrantLock();
    ReentrantLock nodeTasksCancelledMapLock = new ReentrantLock();

    private List<ComputeNodeDAO> computeNodeList = new ArrayList<ComputeNodeDAO>();
    private List<String> taskList = new ArrayList<String>();
    private List<String> pendingTaskList = new ArrayList<String>();
    private List<String> mergeQuededFileList = new ArrayList<String>();

    private HashMap<String, Integer> nodeTasksMap = new HashMap<String, Integer>();
    private HashMap<String, Integer> nodeTasksFailedMap = new HashMap<String, Integer>();
    private HashMap<String, Integer> nodeTasksCancelledMap = new HashMap<String, Integer>();
    private int NO_OF_TASKS_QUEUED = 0;

    public static final String SEPARATOR = "#@#";
    public static final String SORT = "S";
    public static final String MERGE = "M";
    public static final String SORTED = "_sorted";
    public static final String TMP = ".tmp";

    public ServerHandler(Server pServer) {
        this.pServer = pServer;
    }

    public String sort(String fileName) throws org.apache.thrift.TException {
        try {
            long t1 = System.currentTimeMillis();
            long fileSizeInBytes = 0;
            int offset = 0;
            File file = new File(fileName);
            if (file.exists()) {
                fileSizeInBytes = file.length();
                //System.out.println("bytes : " + fileSizeInBytes);

            } else {
                String error = "ERROR : File does not exist";
                System.out.println(error);
                return error;
            }
            mergeQuededFileList.clear();
            // filling sort tasks in taskList
            taskListLock.lock();
            while (offset + Server.CHUNK_SIZE < fileSizeInBytes) {
                for(int i=0; i<Server.REPLICATION_FACTOR; i++)
                    taskList.add(SORT + SEPARATOR + fileName + SEPARATOR + offset + SEPARATOR + Server.CHUNK_SIZE);
                offset += Server.CHUNK_SIZE;
            }
            if (offset != fileSizeInBytes) {
                for(int i=0; i<Server.REPLICATION_FACTOR; i++)
                    taskList.add(SORT + SEPARATOR + fileName + SEPARATOR + offset + SEPARATOR + (fileSizeInBytes - offset));
            }
            taskListLock.unlock();

            // wait for sort to complete
            while(!isSortComplete(fileName));

            while(!isMergeComplete(fileName))
                createMergeTask();

            //rename the last file and delete temporary files
            performCleanUp(fileName);
            System.out.println("Time taken for sort operation : " + (System.currentTimeMillis() - t1) + " milliseconds");
            System.out.println("Sorted File Path : "+fileName+SORTED);
            System.out.println(systemInfo());
        } catch(Exception e) {
            e.printStackTrace();
        }
        return fileName+SORTED;
    }

   /* public String systemInfo() throws org.apache.thrift.TException {
        taskListLock.lock();
        System.out.println("taskList : " + taskList);
        taskListLock.unlock();

        pendingTaskListLock.lock();
        System.out.println("pendingTaskList : " + pendingTaskList);
        pendingTaskListLock.unlock();

        nodeTasksMapLock.lock();
        System.out.println("NO_OF_TASKS_QUEUED : " + NO_OF_TASKS_QUEUED);
        nodeTasksMapLock.unlock();

        nodeTasksFailedMapLock.lock();
        System.out.println("taskList : " + taskList);
        nodeTasksFailedMapLock.unlock();

        nodeTasksCancelledMapLock.lock();
        System.out.println("taskList : " + taskList);
        nodeTasksCancelledMapLock.unlock();

        return "";
    }*/

    public String systemInfo() throws org.apache.thrift.TException {
        StringBuffer info = new StringBuffer();
        Integer failedCount = null; Integer cancelledCount = null;
        for(Map.Entry<String, Integer> pEntry : nodeTasksMap.entrySet()) {
            failedCount = nodeTasksFailedMap.get(pEntry.getKey());
            if(failedCount == null) failedCount = 0;
            cancelledCount = nodeTasksCancelledMap.get(pEntry.getKey());
            if(cancelledCount == null) cancelledCount = 0;
            info.append("**********************************************\n");
            info.append(" Compute Node : " + pEntry.getKey()+"\n");
            info.append(" Total tasks : " + pEntry.getValue()+"\n");
            info.append(" Cancelled tasks : " + cancelledCount+"\n");
            info.append(" Failed tasks : " + failedCount+"\n");
        }
        info.append("**********************************************\n");
        return info.toString();
    }

    public boolean joinSystem(ComputeNodeDAO computeNode) throws org.apache.thrift.TException {
        computeNodeListLock.lock();
        computeNodeList.add(computeNode);
        computeNodeListLock.unlock();
        for(int i = 0; i<NO_THREADS_PER_NODE; i++) {
            Thread pThread = new Thread(new ComputeNodeThread(computeNode, this));
            pThread.start();
        }
        return true;
    }

    public String getTask(String node) {
        String task = null;
        taskListLock.lock();
        if(!taskList.isEmpty()) {
            //System.out.println("*******************************************");
            //System.out.println("getTask() : " + taskList);
            task = taskList.remove(0);
            //System.out.println("getTask() : " + taskList);
            //System.out.println("*******************************************");
        }
        taskListLock.unlock();

        if(task != null)
            incrementTaskCount(node);
        return task;
    }

    public void addTask(String task) {
        taskListLock.lock();
        //System.out.println("*******************************************");
        //System.out.println("addTask() : " + taskList);
        taskList.add(task);
        //System.out.println("addTask() : " + taskList);
        //System.out.println("*******************************************");
        taskListLock.unlock();
    }

    public void addPendingTask(String task) {
        pendingTaskListLock.lock();
        //System.out.println("*******************************************");
        //System.out.println("pendingTaskList : " + pendingTaskList);
        String outputFile = task.substring(0, task.indexOf(TMP));
        if(!mergeQuededFileList.contains(outputFile)) {
            if(renameFile(task, outputFile)) {
                pendingTaskList.add(outputFile);
                mergeQuededFileList.add(outputFile);
            }
        }
        //System.out.println("pendingTaskList : " + pendingTaskList);
        //System.out.println("*******************************************");
        pendingTaskListLock.unlock();
    }

    private boolean renameFile(String tempFileStr, String outputFileStr) {
        try {
            //System.out.println("Rename request - "+tempFileStr+" : "+outputFileStr+" : "+new File(tempFileStr).length());
            File outputFile = new File(outputFileStr);
            while(!outputFile.exists()) {
                new File(tempFileStr).renameTo(outputFile);
            }
            //System.out.println("Rename request completed - "+tempFileStr+" : "+outputFileStr);
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public void removeNode(ComputeNodeDAO pComputeNode) {
        System.out.println("Removing node from the system : " + pComputeNode.getIp() + ":" + pComputeNode.getPort());
        computeNodeListLock.lock();
        computeNodeList.remove(pComputeNode);
        computeNodeListLock.unlock();
    }

    public void incrementTaskCount(String node) {
        nodeTasksMapLock.lock();
        Integer count = nodeTasksMap.get(node);
        if(count == null) count = 0;
        count++;
        nodeTasksMap.put(node, count);
        NO_OF_TASKS_QUEUED++;
        nodeTasksMapLock.unlock();
    }

    public void decrementTaskCount(String node) {
        nodeTasksMapLock.lock();
        NO_OF_TASKS_QUEUED--;
        nodeTasksMapLock.unlock();
    }

    public void incrementFailedTaskCount(String node) {
        nodeTasksFailedMapLock.lock();
        Integer count = nodeTasksFailedMap.get(node);
        if(count == null) count = 0;
        count++;
        nodeTasksFailedMap.put(node, count);
        nodeTasksFailedMapLock.unlock();
    }

    public void incrementCancelledTaskCount(String node) {
        nodeTasksCancelledMapLock.lock();
        Integer count = nodeTasksCancelledMap.get(node);
        if(count == null) count = 0;
        count++;
        nodeTasksCancelledMap.put(node, count);
        nodeTasksCancelledMapLock.unlock();
    }

    private boolean isSortComplete(String fileName) {
        taskListLock.lock();
        nodeTasksMapLock.lock();
        pendingTaskListLock.lock();
        boolean isComplete = taskList.isEmpty();
        if(isComplete) {
            isComplete = NO_OF_TASKS_QUEUED == 0;
        }
        if(isComplete) {
            long fileSize = new File(fileName).length();
            long noOfTasks = fileSize % Server.CHUNK_SIZE == 0 ? fileSize / Server.CHUNK_SIZE : fileSize / Server.CHUNK_SIZE + 1;
            isComplete = pendingTaskList.size() == noOfTasks;
        }

        pendingTaskListLock.unlock();
        nodeTasksMapLock.unlock();
        taskListLock.unlock();
        return isComplete;
    }

    private boolean isMergeComplete(String fileName) {
        taskListLock.lock();
        nodeTasksMapLock.lock();
        pendingTaskListLock.lock();
        boolean isComplete = taskList.isEmpty() && pendingTaskList.size() == 1 && NO_OF_TASKS_QUEUED == 0;
        if(isComplete) {
            File origFile = new File(fileName);
            File sortedFile = new File(pendingTaskList.get(0));
            isComplete = Math.abs(origFile.length() - sortedFile.length()) < 5;
        }
        pendingTaskListLock.unlock();
        nodeTasksMapLock.unlock();
        taskListLock.unlock();
        return isComplete;
    }

    /*private boolean isSortComplete(String fileName) {
        taskListLock.lock();
        boolean isComplete = taskList.isEmpty();
        taskListLock.unlock();

        if(isComplete) {
            nodeTasksMapLock.lock();
            isComplete = NO_OF_TASKS_QUEUED == 0;
            nodeTasksMapLock.unlock();
        }

        if(isComplete) {
            pendingTaskListLock.lock();
            long fileSize = new File(fileName).length();
            long noOfTasks = fileSize % Server.CHUNK_SIZE == 0 ? fileSize / Server.CHUNK_SIZE : fileSize / Server.CHUNK_SIZE + 1;
            isComplete = pendingTaskList.size() == noOfTasks;
            pendingTaskListLock.unlock();
        }

        return isComplete;
    }

    private boolean isMergeComplete(String fileName) {
        taskListLock.lock();
        boolean isComplete = taskList.isEmpty();
        taskListLock.unlock();

        if(isComplete) {
            nodeTasksMapLock.lock();
            isComplete = NO_OF_TASKS_QUEUED == 0;
            nodeTasksMapLock.unlock();
        }

        if(isComplete) {
            pendingTaskListLock.lock();
            File origFile = new File(fileName);
            File sortedFile = new File(pendingTaskList.get(0));
            isComplete = Math.abs(origFile.length() - sortedFile.length()) < 5;
            pendingTaskListLock.unlock();
        }
        return isComplete;
    }*/

    private void createMergeTask() {
        String fileListStr = "";
        pendingTaskListLock.lock();
        int size = Math.min(Server.MERGE_SIZE, pendingTaskList.size());
        if(size > 1) {
            for (int i = 0; i < size; i++) {
                if (i == 0)
                    fileListStr = pendingTaskList.remove(0);
                else
                    fileListStr = fileListStr + SEPARATOR + pendingTaskList.remove(0);
            }
        }
        pendingTaskListLock.unlock();

        if(size > 1) {
            for(int i=0; i<Server.REPLICATION_FACTOR; i++)
                addTask(MERGE + SEPARATOR + fileListStr);
        }
    }

    private void performCleanUp(String fileName) {
        // renaming final file
        pendingTaskListLock.lock();
        File finalFile = new File(pendingTaskList.remove(0));
        File newFile = new File(fileName+SORTED);
        finalFile.renameTo(newFile);
        pendingTaskListLock.unlock();

        // deleting all the temporary generated files
        File inputFile = new File(fileName);
        File inputFileDir = inputFile.getParentFile();
        for(File pFile : inputFileDir.listFiles()) {
            //System.out.println("pFile.getAbsolutePath() : "+pFile.getAbsolutePath());
            if(!pFile.equals(newFile) && !pFile.equals(inputFile) && pFile.getAbsolutePath().startsWith(fileName+"_")) {
                pFile.delete();
                //System.out.println("pFile.delete() : " + pFile.delete());
            }
        }
    }

}

class ComputeNodeThread implements Runnable {

    int timeout = 180000;
    private ComputeNodeDAO pComputeNode;
    private ServerHandler pServerHandler;

    public ComputeNodeThread(ComputeNodeDAO pComputeNode, ServerHandler pServerHandler) {
        this.pComputeNode = pComputeNode;
        this.pServerHandler = pServerHandler;
    }

    public void run() {
        String task = null; String[] taskDescArr = null; String retFileName = null;
        TTransport transport = new TSocket(pComputeNode.getIp(), pComputeNode.getPort(), timeout);
        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
        ComputeNodeService.Client computeNodeClient = new ComputeNodeService.Client(protocol);
        try {
            transport.open();
            while (true) {
                task = pServerHandler.getTask(pComputeNode.getIp()+":"+pComputeNode.getPort());
                if(task != null) {
                    //pServerHandler.incrementTaskCount(pComputeNode.getIp()+":"+pComputeNode.getPort());
                    taskDescArr = task.split(ServerHandler.SEPARATOR);
                    try {
                        if (ServerHandler.SORT.equals(taskDescArr[0])) {
                            retFileName = computeNodeClient.sort(taskDescArr[1], Long.parseLong(taskDescArr[2]), Integer.parseInt(taskDescArr[3]));
                        } else if (ServerHandler.MERGE.equals(taskDescArr[0])) {
                            List fileList = new ArrayList(Arrays.asList(taskDescArr));
                            fileList.remove(0); //remove task description
                            retFileName = computeNodeClient.merge(fileList);
                        }
                        if(!"NULL".equals(retFileName))
                            pServerHandler.addPendingTask(retFileName);
                        else
                            pServerHandler.incrementCancelledTaskCount(pComputeNode.getIp() + ":" + pComputeNode.getPort());
                        pServerHandler.decrementTaskCount(pComputeNode.getIp() + ":" + pComputeNode.getPort());
                    } catch (Exception e) {
                        if(e instanceof org.apache.thrift.TApplicationException && e.getMessage().contains("out of sequence response")) {
                            System.out.println("Out of sequence response");
                        }else {
                            pServerHandler.incrementFailedTaskCount(pComputeNode.getIp() + ":" + pComputeNode.getPort());
                            if(e instanceof TaskFailureException) {
                                pServerHandler.addTask(task);
                                pServerHandler.decrementTaskCount(pComputeNode.getIp() + ":" + pComputeNode.getPort());
                            }else if(e.getCause() instanceof java.net.SocketTimeoutException) {
                                System.out.println("SocketTimeoutException !");
                                pServerHandler.addTask(task);
                                pServerHandler.decrementTaskCount(pComputeNode.getIp() + ":" + pComputeNode.getPort());
                            } else if(e instanceof org.apache.thrift.transport.TTransportException) {
                                System.out.println("TTransportException !");
                                pServerHandler.addTask(task);
                                pServerHandler.removeNode(pComputeNode);
                                pServerHandler.decrementTaskCount(pComputeNode.getIp() + ":" + pComputeNode.getPort());
                                break;
                            }else {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
            if(e instanceof org.apache.thrift.transport.TTransportException) {
                pServerHandler.removeNode(pComputeNode);
            }
        }
        transport.close();
    }
}