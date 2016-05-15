service FileServerService {
	string synch(),
	string write(1: string fileName, 2: string contents),
	string read(1: string fileName),
	string getSystemInfo(),	
	bool updateCoordinator(1: string ip, 2: i32 port),
	string writeFromCoord(1: string fileName, 2: string contents),
	string readFromCoord(1: string fileName),
	map<string,string> getFileMapFromCoord(),
	
}
