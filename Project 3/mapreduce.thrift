exception TaskFailureException {
 	1: string msg
}

struct ComputeNodeDAO {
    1: string ip,
    2: i32 port,
}

service ServerService {   
    string sort(1: string fileName),
    string systemInfo(),
    bool joinSystem(1: ComputeNodeDAO computeNode),
}

service ComputeNodeService {
    string sort(1: string fileName, 2: i64 offset, 3: i32 chunkSize) throws (1: TaskFailureException taskFailureException),
    string merge(1: list<string> fileNameList) throws (1: TaskFailureException taskFailureException),
}
