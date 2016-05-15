import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;

import java.net.InetAddress;
import java.security.SecurityPermission;

/**
 * Created by sarda014 on 2/7/16.
 */
public class Client {

    public static String SEPARATOR = "#@#";
    public static String NULL = "NULL";

    public static void main(String args[]) throws Exception {

        String SERVER_IP = "128.101.37.38";
        int SERVER_PORT = 9092;

        if(args.length < 3)
            System.out.println("Please provide correct number of arguments");
        SERVER_IP = args[0];
        SERVER_PORT = Integer.parseInt(args[1]);
        String operation = args[2];

        if(("W".equals(operation) && args.length != 5) || ("R".equals(operation) && args.length != 4))
            System.out.println("Please provide correct number of arguments");

        //Create client connect.
        InetAddress address = InetAddress.getByName(SERVER_IP);
        SERVER_IP = address.getHostAddress();

        TTransport transport = new TSocket(SERVER_IP, SERVER_PORT);
        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
        FileServerService.Client fileServerclient = new FileServerService.Client(protocol);
        //Try to connect
        transport.open();

        long t1 = System.currentTimeMillis();

        if("R".equals(operation)){
            String[] readContentArr = fileServerclient.read(args[3]).split(SEPARATOR);
            System.out.println("Read | Contents : " + (NULL.equals(readContentArr[0]) ? "File not found." : readContentArr[0]));
            System.out.println("Read | Version : " + readContentArr[1]);
        }else if("W".equals(operation)){
            fileServerclient.write(args[3], args[4]);
        }else if("S".equals(operation)){
            System.out.println(fileServerclient.synch());
        }else if("I".equals(operation)){
            System.out.println(fileServerclient.getSystemInfo());
        }

        System.out.println("Time taken : "+(System.currentTimeMillis()-t1)+" milliseconds");

        transport.close();
    }

}
