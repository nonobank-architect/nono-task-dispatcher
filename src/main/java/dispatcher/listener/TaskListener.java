package dispatcher.listener;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import dispatcher.service.SchedulerService;

public class TaskListener implements IZkChildListener{

	private SchedulerService service;
	
	private DataListener dataListener;
	private static Logger logger = Logger.getLogger(TaskListener.class);
	public TaskListener(SchedulerService service){
		this.service=service;
		dataListener=new DataListener(service);
	}
	
	@Override
	public void handleChildChange(String parentPath, List<String> currentChilds)
			throws Exception {
		ZkClient zkClient=service.zkClient;
		zkClient.unsubscribeAll();
		zkClient.subscribeChildChanges(parentPath, this);
		if(currentChilds!=null && currentChilds.size()>0)
		for(int i=0;i<currentChilds.size();i++){
			zkClient.subscribeDataChanges(parentPath+"/"+currentChilds.get(i), dataListener);
		}
		service.getSchedulerTaskList(zkClient);
		logger.error("tasks changed");
	}

}
