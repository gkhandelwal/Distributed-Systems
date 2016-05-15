import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by sarda014 on 3/17/16.
 */

public class FileServer {

    private String ip;
    private int port;
    private HashMap<String, String> fileNameContentsMap = new HashMap<String, String>();

    public static String COORD_IP = "128.101.37.38";
    public static int COORD_PORT = 9090;
    private static int PORT = 9092;
    public static String NULL = "NULL";

    //coordinator
    public static int NR = 2;
    public static int NW = 2;
    private List<FileServer> fileServerList = new ArrayList<FileServer>();


    public static void main(String[] args) throws Exception {
        if(args.length < 5)
            System.out.println("Please provide correct number of arguments");
        String coordIp = args[0];
        COORD_PORT = Integer.parseInt(args[1]);
        PORT = Integer.parseInt(args[2]);
        NR = Integer.parseInt(args[3]);
        NW = Integer.parseInt(args[4]);

        if(NR <= 0 || NW <= 0) {
            System.out.println("NR and NW should be greater than 0");
        }

        InetAddress address = InetAddress.getByName(coordIp);
        COORD_IP = address.getHostAddress();

        Thread thread = new Thread() {
            public void run() {
                try {
                    FileServer pServer = new FileServer();
                    InetAddress addr = InetAddress.getLocalHost();
                    System.out.println(addr.getHostAddress());
                    pServer.setIp(addr.getHostAddress()); // initialize server IP
                    pServer.setPort(PORT); // initialize server port
                    pServer.startServer();
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        };
        thread.start();
    }

    private void startServer() throws TException {
        //TNonblockingServerTransport serverTransport  = new TNonblockingServerSocket(port);
        TServerTransport serverTransport = new TServerSocket(port);
        TTransportFactory factory = new TFramedTransport.Factory();

        if(ip.equals(COORD_IP) && port == COORD_PORT){
            fileServerList.add(this);
        }else {
            notifyCoordinator();
        }

        //Create service request handler
        FileServerHandler handler = new FileServerHandler(this);
        FileServerService.Processor processor = new FileServerService.Processor(handler);

        // start synch process thread
        if(ip.equals(COORD_IP) && port == COORD_PORT){
            Thread pThread = new Thread(new SynchThread(handler));
            pThread.start();
        }

        //Set server arguments
        //TServer.Args serverArgs = new TServer.Args(serverTransport);
        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
        serverArgs.processor(processor);
        serverArgs.transportFactory(factory);

        //TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));
        TServer server = new TThreadPoolServer(serverArgs);
        server.serve();
    }

    private void notifyCoordinator() throws org.apache.thrift.TException {
        TTransport transport = new TSocket(COORD_IP, COORD_PORT);
        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
        FileServerService.Client coordClient = new FileServerService.Client(protocol);
        transport.open();
        coordClient.updateCoordinator(ip, port);
        transport.close();
    }


    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public HashMap<String, String> getFileNameContentsMap() {
        return fileNameContentsMap;
    }

    public void setFileNameContentsMap(HashMap<String, String> fileNameContentsMap) {
        this.fileNameContentsMap = fileNameContentsMap;
    }

    public List<FileServer> getFileServerList() {
        return fileServerList;
    }

    public void setFileServerList(List<FileServer> fileServerList) {
        this.fileServerList = fileServerList;
    }

    public String toString() {
        return ip+":"+port;
    }
}

class SynchThread implements Runnable {

    private FileServerHandler coordHandler;

    public SynchThread(FileServerHandler coordHandler) {
        this.coordHandler = coordHandler;
    }

    public void run() {
        try {
            while (true) {
                Thread.sleep(30000); // sleep for 30 seconds
                coordHandler.synch();
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
