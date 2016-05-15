struct NodeDAO {
	1: string nodeIP,
	2: i32 nodePort,
	3: i32 nodeId,
}

service SuperNodeService {
	list<NodeDAO> Join(1: string IP, 2: i32 Port),
	bool PostJoin(1: string IP, 2: i32 Port),
	NodeDAO GetNode(),
	string getDHTInfo(),
}

service NodeService {
	string Write(1: string Filename, 2: string Contents),
	string Read(1: string Filename),
	bool UpdateDHT(1: list<NodeDAO> NodesList),
    string getNodeInfo(),
}
