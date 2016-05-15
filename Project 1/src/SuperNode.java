import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.*;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SuperNode implements SuperNodeService.Iface 
{
	public static int SUPER_NODE_PORT = 9090;
	public static int NO_OF_NODES = 32;

	private Lock lock = new ReentrantLock();
	private String servingIP;
	private int servingPort;
	private  List<NodeDAO> nodes = new ArrayList<NodeDAO>();
	private int numOfNodes=0;
	private static ArrayList<Integer> listToAssignNodeId = new ArrayList<Integer> ();

	public List<NodeDAO> Join(String IP, int Port)
	{
		//acquire lock or do synchronize but this might not work.. needs to set flag or stage
		if (lock.tryLock())
		{
			// return list of nodes in system...
			System.out.println("Join request for : "+IP+":"+Port);
			servingIP = IP;
			servingPort = Port;
			NodeDAO temp = new NodeDAO();
			List<NodeDAO> tempNodes = new ArrayList<NodeDAO>(nodes);
			temp.nodeIP = IP;
			temp.nodePort = Port;
			temp.nodeId = listToAssignNodeId.get(numOfNodes);
			tempNodes.add(temp);
			Collections.sort(tempNodes, new Comparator<NodeDAO>() {
				@Override
				public int compare(NodeDAO o1, NodeDAO o2) {
					return new Integer(o1.getNodeId()).compareTo(new Integer(o2.getNodeId()));
				}
			});
			return tempNodes;
		}
		else
		{
		    // need to return NACK
		    return null;
		}
	}
	public boolean PostJoin(String IP, int Port)
	{
		if(servingIP.equals(IP) && servingPort == Port)
		{
			try {
				NodeDAO temp = new NodeDAO();
				temp.nodeIP = IP;
				temp.nodePort = Port;
				temp.nodeId = listToAssignNodeId.get(numOfNodes);
				nodes.add(temp);
				Collections.sort(nodes ,new Comparator<NodeDAO>() {
					@Override
					public int compare(NodeDAO o1, NodeDAO o2) {
						return new Integer(o1.getNodeId()).compareTo(new Integer(o2.getNodeId()));
					}
				});
				numOfNodes++;
				lock.unlock();
				System.out.println("Join request completed for : "+IP+":"+Port);
			}catch(Exception e){
				e.printStackTrace();
			}
			return true;
		}
		return false;
	}
	
	public NodeDAO GetNode()
	{
		//select some random index and pass the node to client
		Random rand = new Random();
		int randomIndex = rand.nextInt((numOfNodes));
		return nodes.get(randomIndex);
	}

	public String getDHTInfo() throws org.apache.thrift.TException {
		TTransport transport; TProtocol protocol; NodeService.Client nodeClient;
		StringBuffer info = new StringBuffer();
		for(NodeDAO pNodeDAO : nodes){
			transport = new TSocket(pNodeDAO.getNodeIP(), pNodeDAO.getNodePort());
			protocol = new TBinaryProtocol(new TFramedTransport(transport));
			nodeClient = new NodeService.Client(protocol);
			transport.open();
			info.append("\n***********************");
			info.append(nodeClient.getNodeInfo());
			transport.close();
		}
		info.append("\n***********************");
		return info.toString();
	}
	
	public static void main(String[] args) throws UnknownHostException
	{
		try
		{
			if(args.length < 1)
				System.out.println("Please provide correct number of arguments");
			SUPER_NODE_PORT = Integer.parseInt(args[0]);

			// filling random node ID's
			for(int i=0;i<NO_OF_NODES;i++) {
				listToAssignNodeId.add(i);
			}
			Collections.shuffle(listToAssignNodeId);

			//Create Thrift server socket
			//TServerTransport serverTransport = new TServerSocket(SUPER_NODE_PORT);
			TNonblockingServerTransport serverTransport  = new TNonblockingServerSocket(SUPER_NODE_PORT);
			TTransportFactory factory = new TFramedTransport.Factory();

			//Create service request handler
			SuperNode handler = new SuperNode();
			SuperNodeService.Processor processor = new SuperNodeService.Processor(handler);

			//Set server arguments
			TServer.Args serverArgs = new TServer.Args(serverTransport);
			serverArgs.processor(processor);	//Set handler
			serverArgs.transportFactory(factory);  //Set FramedTransport (for performance)

			//Run server as a single thread
			//TServer server = new TSimpleServer(serverArgs);
			TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));
			InetAddress IP = InetAddress.getLocalHost();
			System.out.println("\n SuperNode IP is :"+IP.getHostAddress());
			server.serve();

		}
		catch(TTransportException e)
		{
			e.printStackTrace();
		}
	}
}
