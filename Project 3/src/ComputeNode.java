import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;

import java.net.InetAddress;

/**
 * Created by sarda014 on 4/15/16.
 */
public class ComputeNode {

    public static String SERVER_IP = "128.101.37.66";
    public static int SERVER_PORT = 9090;
    private static int PORT = 9094;
    private static double FAIL_PROBAB = 0.0;

    private String ip;
    private int port;
    private double failProbability = 0.0;

    public static void main(String[] args) throws Exception {
        if(args.length < 4)
            System.out.println("Please provide correct number of arguments");
        SERVER_IP = args[0];
        SERVER_PORT = Integer.parseInt(args[1]);
        PORT = Integer.parseInt(args[2]);
        FAIL_PROBAB = Double.parseDouble(args[3]);

        Thread thread = new Thread() {
            public void run() {
                try {
                    ComputeNode pNode = new ComputeNode();
                    InetAddress addr = InetAddress.getLocalHost();
                    System.out.println(addr.getHostAddress());
                    pNode.setIp(addr.getHostAddress()); // initialize server IP
                    pNode.setPort(PORT); // initialize server port
                    pNode.setFailProbability(FAIL_PROBAB);
                    pNode.startNode();
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        };
        thread.start();
    }

    private void startNode() throws TException {
        TServerTransport serverTransport = new TServerSocket(port);
        TTransportFactory factory = new TFramedTransport.Factory();

        notifyServer();

        //Create service request handler
        ComputeNodeHandler handler = new ComputeNodeHandler(this);
        ComputeNodeService.Processor processor = new ComputeNodeService.Processor(handler);

        //Set server arguments
        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
        serverArgs.processor(processor);
        serverArgs.transportFactory(factory);

        TServer server = new TThreadPoolServer(serverArgs);
        server.serve();
    }

    private void notifyServer() throws org.apache.thrift.TException {
        TTransport transport = new TSocket(SERVER_IP, SERVER_PORT);
        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
        ServerService.Client serverClient = new ServerService.Client(protocol);
        transport.open();
        serverClient.joinSystem(new ComputeNodeDAO(ip, port));
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

    public double getFailProbability() {
        return failProbability;
    }

    public void setFailProbability(double failProbability) {
        this.failProbability = failProbability;
    }
}
