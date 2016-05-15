import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;

import java.net.InetAddress;
import java.util.List;

/**
 * Created by sarda014 on 4/15/16.
 */

public class Server {

    public static int PORT = 9090;
    public static int CHUNK_SIZE = 162926;
    public static int MERGE_SIZE = 8;
    public static int REPLICATION_FACTOR = 1;

    public static void main(String[] args) throws Exception {
        if(args.length < 4)
            System.out.println("Please provide correct number of arguments");
        PORT = Integer.parseInt(args[0]);
        CHUNK_SIZE = Integer.parseInt(args[1]);
        MERGE_SIZE = Integer.parseInt(args[2]);
        REPLICATION_FACTOR = Integer.parseInt(args[3]);

        Thread thread = new Thread() {
            public void run() {
                try {
                    Server pServer = new Server();
                    InetAddress addr = InetAddress.getLocalHost();
                    System.out.println(addr.getHostAddress());
                    pServer.startServer();
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        };
        thread.start();
    }

    public void startServer() throws TException {
        TServerTransport serverTransport = new TServerSocket(PORT);
        TTransportFactory factory = new TFramedTransport.Factory();

        //Create service request handler
        ServerHandler handler = new ServerHandler(this);
        ServerService.Processor processor = new ServerService.Processor(handler);

        //Set server arguments
        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
        serverArgs.processor(processor);
        serverArgs.transportFactory(factory);

        System.out.println("Server started ...");

        TServer server = new TThreadPoolServer(serverArgs);
        server.serve();
    }

}
