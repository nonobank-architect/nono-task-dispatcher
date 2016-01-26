package com.nonobank.scheduler.listener;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.apache.log4j.Logger;

import com.nonobank.scheduler.service.SchedulerService;


public class SchedulerListener implements IZkChildListener{

	private SchedulerService service;
	private static Logger logger = Logger.getLogger(SchedulerListener.class);
	public SchedulerListener(SchedulerService service){
		this.service=service;
	}
	@Override
	public void handleChildChange(String parentPath, List<String> currentChilds)
			throws Exception {
		service.getSchedulerTaskList();
		logger.error("scheduler changed");
	}

}
