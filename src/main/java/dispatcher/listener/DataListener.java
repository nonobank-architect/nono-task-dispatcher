package dispatcher.listener;


import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import dispatcher.service.SchedulerService;
import dispatcher.service.TaskService;

public class DataListener  implements IZkDataListener{

	private SchedulerService service;
	private static Logger logger = Logger.getLogger(DataListener.class);
	
	public DataListener(SchedulerService service){
		this.service=service;
	}
	@Override
	public void handleDataChange(String dataPath, Object data) throws Exception {
		ZkClient zkClient=service.zkClient;
		service.getSchedulerTaskList(zkClient);
		logger.error("data changed"+dataPath);
	}

	@Override
	public void handleDataDeleted(String dataPath) throws Exception {
		
	}

	

}
