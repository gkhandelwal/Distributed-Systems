import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by sarda014 on 2/20/16.
 */

public class NodeHandler implements NodeService.Iface {

    Node pNode = null;

    public NodeHandler(Node pNode) {
        this.pNode = pNode;
    }

    public String Write(String Filename, String Contents) throws org.apache.thrift.TException {
        System.out.println("Write Request ..");
        String path = null;
        int fileNodeId = getNodeIdFromFileName(Filename);
        if(checkIfCurrentNode(fileNodeId)){
            path = pNode.getNodeId()+"";
            pNode.getFileNameContentsMap().put(Filename, Contents);
        }else{
            try {
                NodeDAO nextNode = getNextNode(fileNodeId);
                TTransport transport = new TSocket(nextNode.getNodeIP(), nextNode.getNodePort());
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                NodeService.Client nodeClient = new NodeService.Client(protocol);
                transport.open();
                path = nodeClient.Write(Filename, Contents);
                path = pNode.getNodeId() + "->" + path;
                transport.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("Write Request Done; Path : "+path);
        return path;
    }

    public String Read(String Filename) throws org.apache.thrift.TException {
        System.out.println("Read Request ..");
        String contents = null; String path = null;
        int fileNodeId = getNodeIdFromFileName(Filename);
        if(checkIfCurrentNode(fileNodeId)){
            path = pNode.getNodeId()+"";
            if(!pNode.getFileNameContentsMap().containsKey(Filename))
                contents = "Error - The requested file name does not exist on the server";
            else
                contents = pNode.getFileNameContentsMap().get(Filename);
        }else{
            NodeDAO nextNode = getNextNode(fileNodeId);
            TTransport transport = new TSocket(nextNode.getNodeIP(), nextNode.getNodePort());
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            NodeService.Client nodeClient = new NodeService.Client(protocol);
            transport.open();
            String[] retVal = nodeClient.Read(Filename).split(Node.SEPARATOR);
            contents = retVal[0];
            path = pNode.getNodeId() + "->" + retVal[1];
            transport.close();
        }
        System.out.println("Read Request Done; Contents : "+contents);
        return contents+Node.SEPARATOR+path;
    }

    public boolean UpdateDHT(List<NodeDAO> NodesList) throws org.apache.thrift.TException {
        System.out.println("UpdateDHT Request .."+NodesList);

        // set predecessor and successor links
        for(int i=0; i<NodesList.size(); i++){
            NodeDAO pNodeDAO = NodesList.get(i);
            if(pNodeDAO.getNodeId() == pNode.getNodeId()){
                pNode.setPredecessor(i == 0 ? NodesList.get(NodesList.size() - 1) : NodesList.get(i - 1));
                pNode.setSuccessor(i == NodesList.size() - 1 ? NodesList.get(0) : NodesList.get(i+1));
            }
        }

        // update finger table
        NodeDAO ftEntry;
        List<NodeDAO> fingerTable = new ArrayList<NodeDAO>();
        int ft_size = (int)Math.ceil(Math.log(Node.NO_OF_NODES) / Math.log(2));
        ft_size = NodesList.size() < ft_size ? NodesList.size()-1 : ft_size;

        for(int i=0; i<ft_size; i++){
            ftEntry = successor((pNode.getNodeId() + (int)Math.pow(2, i)) % Node.NO_OF_NODES, NodesList);
            fingerTable.add(ftEntry);
        }
        pNode.setFingerTable(fingerTable);
        System.out.println("UpdateDHT Done; Updated Node Info :"+getNodeInfo());
        return true;
    }

    public String getNodeInfo() {
        StringBuffer info = new StringBuffer();
        String ftStr = "";
        for(NodeDAO pNodeDAO : pNode.getFingerTable())
            ftStr += pNodeDAO.getNodeId() + ", ";
        if(ftStr.length() > 2) ftStr = ftStr.substring(0, ftStr.length()-2);
        info.append("\nNodeId : "+pNode.getNodeId());
        info.append("\nPredecessor : "+pNode.getPredecessor().getNodeId());
        info.append("\nSuccessor : "+pNode.getSuccessor().getNodeId());
        info.append("\nFinger Table Entries : "+ftStr);
        info.append("\nNumber of Files : "+pNode.getFileNameContentsMap().size());
        return info.toString();
    }

    private boolean checkIfCurrentNode(int fileNodeId) {
        int predNodeId = pNode.getPredecessor().getNodeId();
        int nodeId = pNode.getNodeId();
        if(fileNodeId < predNodeId) fileNodeId += Node.NO_OF_NODES;
        if(nodeId < predNodeId) nodeId += Node.NO_OF_NODES;
        //System.out.println("predNodeId : "+predNodeId+" fileNodeId : "+fileNodeId+" nodeId : "+nodeId);
        return predNodeId <= fileNodeId && fileNodeId <= nodeId;
    }

    private NodeDAO successor(int nodeId, List<NodeDAO> nodeList){
        NodeDAO succNode = nodeList.get(0);
        for(NodeDAO pNodeDAO : nodeList){
            if(pNodeDAO.getNodeId() >= nodeId){
                succNode = pNodeDAO;
                break;
            }
        }
        return succNode;
    }

    private int getNodeIdFromFileName(String fileName) {
        return pNode.getFingerTable().isEmpty() ? pNode.getNodeId() : Math.abs(fileName.hashCode()) % Node.NO_OF_NODES;
    }

    private NodeDAO getNextNode(int fileNodeId) {
        int i; int ftNodeId = 0;
        List<NodeDAO> ftList = pNode.getFingerTable();
        if(fileNodeId < pNode.getNodeId()) fileNodeId = fileNodeId + Node.NO_OF_NODES;
        for(i=0; i<ftList.size(); i++){
            NodeDAO pNodeDAO = ftList.get(i);
            ftNodeId = pNodeDAO.getNodeId() < pNode.getNodeId() ? pNodeDAO.getNodeId() + Node.NO_OF_NODES : pNodeDAO.getNodeId();
            if(ftNodeId > fileNodeId) {
                break;
            }
        }
        if(i!=0) i = i-1;
        return ftList.get(i);
    }

}
