import org.apache.thrift.TException;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by sarda014 on 4/15/16.
 */
public class ComputeNodeHandler implements ComputeNodeService.Iface {

    public static final String MERGE = "_M";
    public static final String TMP = ".tmp";
    private final int MAX_SIZE_CHECK = 99999;
    ComputeNode pComputeNode = null;

    public ComputeNodeHandler(ComputeNode pComputeNode) {
        this.pComputeNode = pComputeNode;
    }

    private final ExecutorService executor = Executors.newFixedThreadPool(5);
    private boolean taskCancelled = false;
    private String tempFileName = null;

    public String sort(String fileName, long offset, int chunkSize) throws org.apache.thrift.TException, TaskFailureException {
        System.out.println("Sort request - fileName : " + fileName + " offset : " + offset + " chunkSize : " + chunkSize);
        tryNodeFailure();
        Future<String> future = null; String outFileName = null; String retFileName = "NULL";
        try{
            //Thread.sleep(5000);
            taskCancelled = false;
            outFileName = fileName + "_" + String.valueOf(offset) + MERGE;
            NodeCallable pNodeCallable = new NodeCallable(this, fileName, offset, chunkSize, null);
            future = executor.submit(pNodeCallable);
            while (!future.isDone()) {
                if(isTaskCompletedByAnotherThread(outFileName)) {
                    taskCancelled = true;
                    future.cancel(false);
                    break;
                }
            }
            //System.out.println("Sort Thread id 1 : "+Thread.currentThread().getId());
            if(!future.isCancelled())
                retFileName = future.get();
            if(retFileName == null) throw new TaskFailureException();
            /*else
                deleteTempFile();*/
            //System.out.println("Sort Thread id 2 : "+Thread.currentThread().getId());
            System.out.println("Sort request completed");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return retFileName;
    }

    public String merge(List<String> fileNameList) throws org.apache.thrift.TException, TaskFailureException {
        System.out.println("Merge request - fileNameList : "+fileNameList);
        tryNodeFailure();
        String outFileName = null; String retFileName = "NULL";
        try{
            //Thread.sleep(5000);
            taskCancelled = false;
            outFileName = fileNameList.get(0) + MERGE;
            NodeCallable pNodeCallable = new NodeCallable(this, null, 0, 0, fileNameList);
            Future<String> future = executor.submit(pNodeCallable);
            while (!future.isDone()) {
                if(isTaskCompletedByAnotherThread(outFileName)) {
                    taskCancelled = true;
                    future.cancel(false);
                    break;
                }
            }
            //System.out.println("Merge Thread id 1 : "+Thread.currentThread().getId());
            if(!future.isCancelled())
                retFileName = future.get();
            if(retFileName == null) throw new TaskFailureException();
            /*else
                deleteTempFile();*/
            //System.out.println("Merge Thread id 2 : "+Thread.currentThread().getId());
            System.out.println("Merge request completed");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return retFileName;
    }

    private boolean isTaskCompletedByAnotherThread(String outFileName) {
        return new File(outFileName).exists();
    }

    public String _sort(String fileName, long offset, int chunkSize) throws org.apache.thrift.TException, TaskFailureException {
        String outputFile = fileName + "_" + String.valueOf(offset) + MERGE;
        String tempFile = outputFile + TMP + UUID.randomUUID().toString();
        tempFileName = tempFile;
        RandomAccessFile file = null; FileWriter writer = null;
        ArrayList<Integer> list = new ArrayList<Integer>();
        try {
            file = new RandomAccessFile(fileName, "r");
            long fileSizeInBytes = new File(fileName).length();

            //no need to seek offset but its fine
            if(offset == 0)
                file.seek(0);
            else
                file.seek(offset - 1);

            byte [] readBuffer =  new byte[chunkSize + (offset == 0 ? 4 : 5)]; // as the largest number is 9999

            int nBytesRead = file.read(readBuffer);
            String strFromByte = new String(readBuffer);
            strFromByte = strFromByte.substring(0,nBytesRead);

            String result = "";
            //System.out.println("Ideal : |" + strFromByte.substring((offset == 0 ? 0 : 1), strFromByte.length()-4)+"|");

            // do not trim string before this
            int startingIndx = offset != 0 ? strFromByte.indexOf(" ") : 0;

            //System.out.println("Ideal : |" + strFromByte.substring((offset == 0 ? 0 : 1), strFromByte.length()-4)+"|");


            long endingIndx;

            //Who told that we are going to read more than 5 ???????? This does not hold when you are reading at last position
            if(nBytesRead==(chunkSize + (offset == 0 ? 4 : 5)))
            {
                // this is normal case... not a corner case
                // check both these conditions again
                endingIndx = strFromByte.substring(strFromByte.length()-5).indexOf(" "); // Not sure if it is 5 or 6.. think once

                // why this strFromByte.length() - 4 ??????
                endingIndx = endingIndx == -1 ? strFromByte.length(): strFromByte.length() - 4 + endingIndx;

                /// these are correct but do we really need this ????
                endingIndx = (offset + chunkSize >= fileSizeInBytes) ? (fileSizeInBytes - offset) : endingIndx;
                result = strFromByte.substring(startingIndx, (int)endingIndx);
            }
            else
            {
                if(((nBytesRead-1<=chunkSize)&&(offset!=0))|| ((nBytesRead<=chunkSize)&&(offset==0)))
                {

                    // this means this does not even have those extra stuff... and everything here is part of this number..
                    result = strFromByte.substring(startingIndx,nBytesRead);
                    result = result.trim();
                    System.out.println("|"+result.trim()+"|");
                    //System.out.println("bytesRead :" + nBytesRead + "result" + result.length() + "strFromByte:" + strFromByte.length()); //162927

                }
                else
                {
                    endingIndx = strFromByte.substring(chunkSize).indexOf(" "); // Not sure if it is 4 or 5
                    endingIndx = endingIndx == -1 ? strFromByte.length() :  chunkSize + endingIndx;
                    endingIndx = (offset + chunkSize >= fileSizeInBytes) ? (fileSizeInBytes - offset) : endingIndx;
                    result = strFromByte.substring(startingIndx, (int)endingIndx);
                }

            }

            StringTokenizer st = new StringTokenizer(result," ");
            while(st.hasMoreElements())
            {

                list.add(Integer.parseInt((String) st.nextElement()));
            }
            Collections.sort(list);
            writer = new FileWriter(tempFile);
            for(int i=0; i<list.size(); i++) {
                if(i != 0)
                    writer.write(" ");
                writer.write(String.valueOf(list.get(i)));
                //System.out.println(String.valueOf(element));
            }
            writer.flush();
        } catch (Exception e) {
            e.printStackTrace();
            tempFile = null;
        }
        finally
        {
            try {
                if (writer != null)
                    writer.close();
                if(file != null)
                    file.close();

            }catch (Exception e) {
                e.printStackTrace();
            }
            //taskCompleted = renameFile(tempFile, outputFile);
        }

        return tempFile;
    }

    public String _merge(List<String> fileNameList) throws org.apache.thrift.TException, TaskFailureException {
        String outputFile = null; String tempFile = null;
        ArrayList<Scanner> fileHandler = new ArrayList<Scanner>();
        FileWriter writer = null;
        try {
            for (int i = 0; i < fileNameList.size(); i++) {
                Scanner s = null;
                s = new Scanner(new File(fileNameList.get(i)));
                fileHandler.add(s);
            }
            outputFile = fileNameList.get(0) + MERGE;
            tempFile = outputFile + TMP + UUID.randomUUID().toString();
            tempFileName = tempFile;
            writer = new FileWriter(tempFile);
            ArrayList<Integer> listNumber = new ArrayList<Integer>();
            boolean flag = true;
            int min = MAX_SIZE_CHECK;
            int index = 0;
            for (int i = 0; i < fileNameList.size(); i++) {
                if (!fileHandler.get(i).hasNextInt()) {
                    listNumber.add(MAX_SIZE_CHECK);
                } else {
                    listNumber.add(fileHandler.get(i).nextInt());
                    if (min > listNumber.get(i)) {
                        min = listNumber.get(i);
                        index = i;
                    }
                }
            }
            while (min!=MAX_SIZE_CHECK)
            {
                if(!flag){
                    writer.write(" ");
                    writer.flush();
                }
                else
                {
                    flag = false;
                }
                writer.write(String.valueOf(min));
                writer.flush();

                if(fileHandler.get(index).hasNextInt())
                    listNumber.set(index, fileHandler.get(index).nextInt());
                else
                    listNumber.set(index, MAX_SIZE_CHECK);
                min=MAX_SIZE_CHECK;
                index = 0;// not required
                for(int in=0;in<listNumber.size();in++)
                {
                    if(min>listNumber.get(in))
                    {
                        min = listNumber.get(in);
                        index = in;
                    }
                }
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            tempFile = null;
        }
        finally
        {
            try {
                if (writer != null)
                    writer.close();
                for (int i = 0; i < fileNameList.size(); i++) {
                    if (fileHandler.get(i) != null)
                        fileHandler.get(i).close();
                }
            }catch (Exception e) {
                e.printStackTrace();
            }

            //taskCompleted = renameFile(tempFile, outputFile);
        }

        return tempFile;
    }

    private boolean renameFile(String tempFile, String outputFile) {
        try {
            Path movefrom = FileSystems.getDefault().getPath(tempFile);
            Path target = FileSystems.getDefault().getPath(outputFile);
            Files.move(movefrom, target);
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    private void tryNodeFailure() throws TaskFailureException {
        double rand = Math.random();
        if(rand < pComputeNode.getFailProbability()) {
            throw new TaskFailureException("Task Failed");
        }
    }

    private void deleteTempFile() {
        try {
            if (tempFileName != null) {
                File tempFile = new File(tempFileName);
                if (tempFile.getAbsoluteFile().exists()) tempFile.delete();
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}

class NodeCallable implements Callable<String> {

    private ComputeNodeHandler computeNodeHandler;
    private String fileName;
    private long offset;
    private int chunkSize;

    public NodeCallable(ComputeNodeHandler computeNodeHandler, String fileName, long offset, int chunkSize, List<String> fileNameList) {
        this.computeNodeHandler = computeNodeHandler;
        this.fileName = fileName;
        this.offset = offset;
        this.chunkSize = chunkSize;
        this.fileNameList = fileNameList;
    }

    List<String> fileNameList;

    public String call() throws TException {
        String retFileName = null;
        if(fileNameList == null) {
            retFileName = computeNodeHandler._sort(fileName, offset, chunkSize);
        } else {
            retFileName = computeNodeHandler._merge(fileNameList);
        }
        return retFileName;
    }
}