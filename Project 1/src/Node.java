import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.*;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by sarda014 on 2/20/16.
 */

public class Node {

    private String nodeIP;
    private int nodePort;
    private int nodeId;
    private List<NodeDAO> fingerTable;
    private HashMap<String, String> fileNameContentsMap;
    private NodeDAO successor;
    private NodeDAO predecessor;

    public static String SUPER_NODE_IP = "128.101.37.242";
    public static int SUPER_NODE_PORT = 9090;
    public static int NODE_PORT = 9091;
    public static int NO_OF_NODES = 32;
    public static String SEPARATOR = "#@#";

    public static void main(String[] args) throws TException {
        if(args.length < 3)
            System.out.println("Please provide correct number of arguments");
        SUPER_NODE_IP = args[0];
        SUPER_NODE_PORT = Integer.parseInt(args[1]);
        NODE_PORT = Integer.parseInt(args[2]);
            Thread thread = new Thread() {
                public void run() {
                    try {
                        Node pNode = new Node();
                        InetAddress addr = InetAddress.getLocalHost();
                        pNode.setNodeIP(addr.getHostAddress()); // initialize node IP
                        pNode.setNodePort(NODE_PORT); // initialize node port
                        pNode.startNode();
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }
            };
            thread.start();
    }

    private void startNode() throws TException {
        //TServerTransport serverTransport = new TServerSocket(nodePort);
        TNonblockingServerTransport serverTransport  = new TNonblockingServerSocket(nodePort);
        TTransportFactory factory = new TFramedTransport.Factory();

        //Create service request handler
        NodeHandler handler = joinDHT();
        if(handler != null) {
            NodeService.Processor processor = new NodeService.Processor(handler);

            //Set server arguments
            TServer.Args serverArgs = new TServer.Args(serverTransport);
            serverArgs.processor(processor);    //Set handler
            serverArgs.transportFactory(factory);  //Set FramedTransport (for performance)

            //Run server as a single thread
            //TServer server = new TSimpleServer(serverArgs);
            TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));
            server.serve();
        }
    }

    private NodeHandler joinDHT() throws TException {
        TTransport transport; TProtocol protocol; NodeHandler handler = null;
        SuperNodeService.Client superNodeClient; NodeService.Client nodeClient;

        //Create client connect.
        transport = new TSocket(SUPER_NODE_IP, SUPER_NODE_PORT);
        protocol = new TBinaryProtocol(new TFramedTransport(transport));
        superNodeClient = new SuperNodeService.Client(protocol);
        transport.open();
        List<NodeDAO> nodeList = superNodeClient.Join(nodeIP, nodePort);
        transport.close();

        if(nodeList == null){
            System.out.println("SuperNode is busy. Please try again later.");
            return null;
        }

        fileNameContentsMap = new HashMap<String, String>();

        for(NodeDAO pNodeDAO : nodeList){
            if(nodeIP.equals(pNodeDAO.getNodeIP()) && nodePort == pNodeDAO.getNodePort()){
                nodeId = pNodeDAO.getNodeId();
                handler = new NodeHandler(this);
                handler.UpdateDHT(nodeList); // build finger table for this node
            }else{
                transport = new TSocket(pNodeDAO.getNodeIP(), pNodeDAO.getNodePort());
                protocol = new TBinaryProtocol(new TFramedTransport(transport));
                nodeClient = new NodeService.Client(protocol);
                transport.open();
                nodeClient.UpdateDHT(nodeList); // update finger tables of other nodes
                transport.close();
            }
        }

        transport = new TSocket(SUPER_NODE_IP, SUPER_NODE_PORT);
        protocol = new TBinaryProtocol(new TFramedTransport(transport));
        superNodeClient = new SuperNodeService.Client(protocol);
        transport.open();
        boolean a = superNodeClient.PostJoin(nodeIP, nodePort);
        transport.close();

        return handler;
    }

    public String getNodeIP() {
        return nodeIP;
    }

    public void setNodeIP(String nodeIP) {
        this.nodeIP = nodeIP;
    }

    public int getNodePort() {
        return nodePort;
    }

    public void setNodePort(int nodePort) {
        this.nodePort = nodePort;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public List<NodeDAO> getFingerTable() {
        return fingerTable;
    }

    public void setFingerTable(List<NodeDAO> fingerTable) {
        this.fingerTable = fingerTable;
    }

    public HashMap<String, String> getFileNameContentsMap() {
        return fileNameContentsMap;
    }

    public void setFileNameContentsMap(HashMap<String, String> fileNameContentsMap) {
        this.fileNameContentsMap = fileNameContentsMap;
    }

    public NodeDAO getSuccessor() {
        return successor;
    }

    public void setSuccessor(NodeDAO successor) {
        this.successor = successor;
    }

    public NodeDAO getPredecessor() {
        return predecessor;
    }

    public void setPredecessor(NodeDAO predecessor) {
        this.predecessor = predecessor;
    }

}
