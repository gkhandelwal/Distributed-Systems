import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;

/**
 * Created by sarda014 on 2/7/16.
 */
public class Client {

    public static String SEPARATOR = "#@#";
    public static String SUPER_NODE_IP = "128.101.37.242";
    public static int SUPER_NODE_PORT = 9090;

    public static void main(String args[]) throws TException {

        if(args.length < 3)
            System.out.println("Please provide correct number of arguments");

        SUPER_NODE_IP = args[0];
        SUPER_NODE_PORT = Integer.parseInt(args[1]);
        String operation = args[2];

        if(("W".equals(operation) && args.length != 5) || ("R".equals(operation) && args.length != 5))
            System.out.println("Please provide correct number of arguments");

        //Create client connect.
        TTransport transport = new TSocket(SUPER_NODE_IP, SUPER_NODE_PORT);
        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
        SuperNodeService.Client superNodeclient = new SuperNodeService.Client(protocol);
        //Try to connect
        transport.open();

        if("R".equals(operation) || "W".equals(operation)){
            NodeDAO pNodeDAO = superNodeclient.GetNode();
            transport.close();

            transport = new TSocket(pNodeDAO.getNodeIP(), pNodeDAO.getNodePort());
            protocol = new TBinaryProtocol(new TFramedTransport(transport));
            NodeService.Client nodeclient = new NodeService.Client(protocol);
            transport.open();

            if("R".equals(operation)){
                String[] retArr = nodeclient.Read(args[3]).split(SEPARATOR);
                System.out.println("Read | contents : " + retArr[0]);
                System.out.println("Read | path : " + retArr[1]);
            }else{
                String path = nodeclient.Write(args[3], args[4]);
                System.out.println("Write | path : "+path);
            }
            transport.close();
        }else if("I".equals(operation)){
            System.out.println("DHT Info : "+superNodeclient.getDHTInfo());
            transport.close();
        }
    }

}
