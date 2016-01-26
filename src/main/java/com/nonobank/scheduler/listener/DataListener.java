package com.nonobank.scheduler.listener;


import org.I0Itec.zkclient.IZkDataListener;
import org.apache.log4j.Logger;

import com.nonobank.scheduler.service.SchedulerService;

/**
 * task任务节点数据变化watch
 * @author geyingchao
 *
 */
public class DataListener  implements IZkDataListener{

	private SchedulerService service;
	private static Logger logger = Logger.getLogger(DataListener.class);
	
	public DataListener(SchedulerService service){
		this.service=service;
	}
	@Override
	public void handleDataChange(String dataPath, Object data) throws Exception {
		service.getSchedulerTaskList();
		logger.error("data changed"+dataPath);
	}

	@Override
	public void handleDataDeleted(String dataPath) throws Exception {
		
	}

	

}
