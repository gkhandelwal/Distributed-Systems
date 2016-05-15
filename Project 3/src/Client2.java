import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.net.InetAddress;

/**
 * Created by khand052 on 4/15/16.
 */
public class Client2 {
    public static String SEPARATOR = "#@#";
    public static String NULL = "NULL";

    public static void main(String args[]) throws Exception {

        String SERVER_IP = "127.0.0.1";
        int SERVER_PORT = 9090;
        String filepath = "/home/sarda014/Documents/Assignments/5105/PA3/sample_data/test/200000";

        /*if(args.length != 3)
            System.out.println("Please provide correct number of arguments");
        SERVER_IP = args[0];
        SERVER_PORT = Integer.parseInt(args[1]);
        filepath = args[2];*/

        //Getting server IP address from host name
        InetAddress address = InetAddress.getByName(SERVER_IP);
        SERVER_IP = address.getHostAddress();

        TTransport transport = new TSocket(SERVER_IP, SERVER_PORT);
        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
        ServerService.Client serverClient = new ServerService.Client(protocol);
        //Try to connect
        transport.open();

        long t1 = System.currentTimeMillis();

        System.out.println("Input File Path : "+filepath);
        System.out.println("Sent to server for sorting ...");
        System.out.println("Sorted File Path : "+serverClient.systemInfo());
        System.out.println("Time taken : "+(System.currentTimeMillis()-t1)+" milliseconds");

        transport.close();
    }
}
