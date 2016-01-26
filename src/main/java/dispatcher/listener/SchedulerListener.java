package dispatcher.listener;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import dispatcher.service.SchedulerService;

public class SchedulerListener implements IZkChildListener{

	private SchedulerService service;
	private static Logger logger = Logger.getLogger(SchedulerListener.class);
	public SchedulerListener(SchedulerService service){
		this.service=service;
	}
	@Override
	public void handleChildChange(String parentPath, List<String> currentChilds)
			throws Exception {
		ZkClient zkClient=service.zkClient;
		service.getSchedulerTaskList(zkClient);
		logger.error("scheduler changed");
	}

}
