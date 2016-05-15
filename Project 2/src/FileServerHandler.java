import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.security.SecurityPermission;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by sarda014 on 3/17/16.
 */

public class FileServerHandler implements FileServerService.Iface {

    FileServer pServer = null;
    public static String SEPARATOR = "#@#";
    private ReentrantReadWriteLock lock;

    public FileServerHandler(FileServer pServer) {
        lock = new ReentrantReadWriteLock(true);
        this.pServer = pServer;
    }

    public String synch() throws org.apache.thrift.TException {
        System.out.println("Synch process started ...");
        if(!checkSystem()) return "System is not configured properly. Please try again.";
        Random rand = new Random(); int randomIndex = 0;
        ArrayList<Integer> contactedServerList = new ArrayList<Integer>();
        ArrayList<Map<String, String>> fileMapList = new ArrayList<Map<String, String>>();
        int numOfServers = pServer.getFileServerList().size();

        // assemble read quorum and get file contents
        for (int i = 0; i < FileServer.NW; i++) {
            do {
                randomIndex = rand.nextInt((numOfServers));
            } while (contactedServerList.contains(randomIndex));
            contactedServerList.add(randomIndex);

            FileServer serverToContact = pServer.getFileServerList().get(randomIndex);
            TTransport transport = new TSocket(serverToContact.getIp(), serverToContact.getPort());
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            FileServerService.Client pClient = new FileServerService.Client(protocol);
            transport.open();
            lock.readLock().lock();
            fileMapList.add(pClient.getFileMapFromCoord());
            lock.readLock().unlock();
            transport.close();
        }

        // Find the maximum version and content for all the files
        String[] contentArr; String maxVerContent = null;
        Integer maxVersion = null; Integer currVersion = null;
        HashMap<String, Integer> fileNameMaxVerMap = new HashMap<String, Integer>();
        HashMap<String, String> fileNameContentMap = new HashMap<String, String>();
        for(Map<String, String> fileMap : fileMapList) {
            for(Map.Entry<String, String> pEntry : fileMap.entrySet()) {
                contentArr = pEntry.getValue().split(SEPARATOR);
                maxVersion = fileNameMaxVerMap.get(pEntry.getKey());
                if(maxVersion == null) maxVersion = 0;
                currVersion = Integer.parseInt(contentArr[1]);
                if(currVersion > maxVersion){
                    maxVersion = currVersion;
                    maxVerContent = contentArr[0];
                    fileNameMaxVerMap.put(pEntry.getKey(), maxVersion);
                    fileNameContentMap.put(pEntry.getKey(), maxVerContent);
                }
            }
        }

        // update contents map with version number
        for(Map.Entry<String, String> pEntry : fileNameContentMap.entrySet()) {
            pEntry.setValue(pEntry.getValue() + SEPARATOR + fileNameMaxVerMap.get(pEntry.getKey()));
        }

        // Push the maximum version and content for all the files to all the replicas
        for(FileServer serverToContact : pServer.getFileServerList()) {
            TTransport transport = new TSocket(serverToContact.getIp(), serverToContact.getPort());
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            FileServerService.Client pClient = new FileServerService.Client(protocol);
            transport.open();
            lock.writeLock().lock();
            for(Map.Entry<String, String> pEntry : fileNameContentMap.entrySet()) {
                pClient.writeFromCoord(pEntry.getKey(), pEntry.getValue());
            }
            lock.writeLock().unlock();
            transport.close();
        }
        System.out.println("Synch process completed ...");
        return "";
    }

    public boolean updateCoordinator(String ip, int port) throws org.apache.thrift.TException {
        FileServer pFileServer = new FileServer();
        pFileServer.setIp(ip);
        pFileServer.setPort(port);
        pServer.getFileServerList().add(pFileServer);
        System.out.println("updateCoordinator : " + pServer.getFileServerList());
        return true;
    }

    public String write(String fileName, String contents) throws org.apache.thrift.TException {
        System.out.println("Write request .. "+fileName);
        try {
            if (pServer.getIp().equals(FileServer.COORD_IP) && pServer.getPort() == FileServer.COORD_PORT) { //coordinator
                if(!checkSystem()) return "System is not configured properly. Please try again.";
                lock.writeLock().lock();
                System.out.println("Write lock acquired .. " );
                Random rand = new Random(); int randomIndex = 0;
                ArrayList<Integer> contactedServerList = new ArrayList<Integer>();
                String readContent; String[] readContentArr;
                int currVersion = 0; int maxVersion = 0;
                int numOfServers = pServer.getFileServerList().size();

                // assemble the write quorum to find the most updated version and content
                for (int i = 0; i < FileServer.NW; i++) {
                    do {
                        randomIndex = rand.nextInt((numOfServers));
                    } while (contactedServerList.contains(randomIndex));
                    //System.out.println("randomIndex : " + randomIndex);
                    contactedServerList.add(randomIndex);

                    FileServer serverToContact = pServer.getFileServerList().get(randomIndex);
                    TTransport transport = new TSocket(serverToContact.getIp(), serverToContact.getPort());
                    TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                    FileServerService.Client pClient = new FileServerService.Client(protocol);
                    transport.open();
                    readContent = pClient.readFromCoord(fileName);
                    System.out.println("readContent : "+readContent);
                    transport.close();

                    if (!readContent.equals(FileServer.NULL)) {
                        readContentArr = readContent.split(SEPARATOR);
                        currVersion = Integer.parseInt(readContentArr[1]);
                        if (currVersion > maxVersion) {
                            maxVersion = currVersion;
                        }
                    }
                }

                // push the most updated version and content to the write quorum
                maxVersion = maxVersion + 1;
                contents = contents + SEPARATOR + maxVersion;
                for (Integer index : contactedServerList) {
                    FileServer serverToContact = pServer.getFileServerList().get(index);
                    TTransport transport = new TSocket(serverToContact.getIp(), serverToContact.getPort());
                    TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                    FileServerService.Client coordClient = new FileServerService.Client(protocol);
                    transport.open();
                    coordClient.writeFromCoord(fileName, contents);
                    transport.close();
                }
                lock.writeLock().unlock();
            } else {
                // forward the request to coordinator if current server is normal file server
                TTransport transport = new TSocket(FileServer.COORD_IP, FileServer.COORD_PORT);
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                FileServerService.Client coordClient = new FileServerService.Client(protocol);
                transport.open();
                contents = coordClient.write(fileName, contents);
                transport.close();
            }
            System.out.println("write complete ");
        }catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public String read(String fileName) throws org.apache.thrift.TException {
        System.out.println("Read request .. "+fileName);
        String contents = null;
        if(pServer.getIp().equals(FileServer.COORD_IP) && pServer.getPort() == FileServer.COORD_PORT) { //coordinator
            if(!checkSystem()) return "System is not configured properly. Please try again.";
            lock.readLock().lock();
            System.out.println("Read lock acquired.. ");
            Random rand = new Random(); int randomIndex = 0;
            ArrayList<Integer> contactedServerList = new ArrayList<Integer>();
            String readContent; String[] readContentArr;
            int currVersion = 0; int maxVersion = 0; String maxVersionContent = FileServer.NULL+SEPARATOR+maxVersion;
            int numOfServers = pServer.getFileServerList().size();

            // assemble the read quorum to find the most updated version and content
            for(int i=0; i<FileServer.NR; i++) {
                do {
                    randomIndex = rand.nextInt((numOfServers));
                }while(contactedServerList.contains(randomIndex));
                contactedServerList.add(randomIndex);

                FileServer serverToContact = pServer.getFileServerList().get(randomIndex);
                TTransport transport = new TSocket(serverToContact.getIp(), serverToContact.getPort());
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                FileServerService.Client pClient = new FileServerService.Client(protocol);
                transport.open();
                readContent =  pClient.readFromCoord(fileName);
                transport.close();

                if(!readContent.equals(FileServer.NULL)) {
                    readContentArr = readContent.split(SEPARATOR);
                    currVersion = Integer.parseInt(readContentArr[1]);
                    if(currVersion > maxVersion) {
                        maxVersion = currVersion;
                        maxVersionContent = readContent;
                    }
                }
            }
            contents = maxVersionContent;
            lock.readLock().unlock();
        } else {
            // forward the request to coordinator if current server is normal file server
            TTransport transport = new TSocket(FileServer.COORD_IP, FileServer.COORD_PORT);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            FileServerService.Client coordClient = new FileServerService.Client(protocol);
            transport.open();
            contents = coordClient.read(fileName);
            transport.close();
        }
        return contents;
    }

    /*
     * Write called from coordinator, so update the contents in local copy and do not forward the request to coordinator
     */
    public String writeFromCoord(String fileName, String contents) throws org.apache.thrift.TException {
        //System.out.println(pServer.getIp()+":"+pServer.getPort()+" - "+fileName+":"+contents);
        Integer requestVersion = Integer.parseInt(contents.split(SEPARATOR)[1]);
        String existContent = pServer.getFileNameContentsMap().get(fileName);
        Integer existVersion = existContent == null ? 0 : Integer.parseInt(existContent.split(SEPARATOR)[1]);
        if(requestVersion > existVersion)
            pServer.getFileNameContentsMap().put(fileName, contents);
        return "";
    }

    /*
     * Read called from coordinator, so return the contents from local copy and do not forward the request to coordinator
     */
    public String readFromCoord(String fileName) throws org.apache.thrift.TException {
        //System.out.println("readFromCoord : "+pServer.getFileNameContentsMap().get(fileName));
        String content = pServer.getFileNameContentsMap().get(fileName);
        if(content == null) content = FileServer.NULL;
        return content;
    }

    public Map<String,String> getFileMapFromCoord() throws org.apache.thrift.TException {
        return pServer.getFileNameContentsMap();
    }

    public String getSystemInfo() throws org.apache.thrift.TException {
        String info = null;
        if (pServer.getIp().equals(FileServer.COORD_IP) && pServer.getPort() == FileServer.COORD_PORT) { //coordinator
            if(!checkSystem()) return "System is not configured properly. Please try again.";
            StringBuffer infoBuf = new StringBuffer();
            infoBuf.append("**************************************************\n");
            for(FileServer serverToContact : pServer.getFileServerList()) {
                TTransport transport = new TSocket(serverToContact.getIp(), serverToContact.getPort());
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                FileServerService.Client pClient = new FileServerService.Client(protocol);
                infoBuf.append("Server : " + serverToContact.getIp() + ":" + serverToContact.getPort() + "\n");
                transport.open();
                infoBuf.append(getInfoFromMap(pClient.getFileMapFromCoord()));
                transport.close();
                infoBuf.append("**************************************************\n");
            }
            info = infoBuf.toString();
        } else {
            TTransport transport = new TSocket(FileServer.COORD_IP, FileServer.COORD_PORT);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            FileServerService.Client coordClient = new FileServerService.Client(protocol);
            transport.open();
            info = coordClient.getSystemInfo();
            transport.close();
        }
        return info;
    }

    private String getInfoFromMap(Map<String, String> fileContentsMap) {
        String info = ""; String[] readContentArr;
        for(Map.Entry<String, String> pEntry : fileContentsMap.entrySet()) {
            readContentArr = pEntry.getValue().split(SEPARATOR);
            info += "   FileName : " + pEntry.getKey() + " | Version No : " + readContentArr[1] + " | Content : " + readContentArr[0] + "\n";
        }
        return info;
    }

    private boolean checkSystem() {
        int N = pServer.getFileServerList().size();
        return (FileServer.NR + FileServer.NW > N) && (FileServer.NW > N/2) && N >= FileServer.NR && N >= FileServer.NW;
    }

}
