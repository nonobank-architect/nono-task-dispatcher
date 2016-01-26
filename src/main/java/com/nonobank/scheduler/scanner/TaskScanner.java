package com.nonobank.scheduler.scanner;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import com.nonobank.scheduler.entity.SchedulerTask;
import com.nonobank.scheduler.service.SchedulerService;
import com.nonobank.scheduler.util.ConfigUtil;
import com.nonobank.scheduler.util.CronParserUtil;


public class TaskScanner implements Runnable {

	private static Logger logger = Logger.getLogger(TaskScanner.class);
	public static SchedulerService service=SchedulerService.newInstance();
	
	public void run() {
		List<SchedulerTask> tasklist=SchedulerService.schedulerTaskList;
	 ZkClient zkClient= SchedulerService.zkClient;
	 Date time=new Date();
	 if(tasklist!=null && tasklist.size()>0){
		  for(int i=0;i<tasklist.size();i++){
			  //扫描task列表，解析cron表达式，判断任务是否应该执行
			  SchedulerTask task=tasklist.get(i);
			  //判断是否需要更新节点数据
			  int status=CronParserUtil.isExecutable(task,time);
			  if(status>0){
				  //正常调用
				  if(status==1){
					  CronParserUtil.updateTask(task); 
				  }
				  //判断调整后的时间是否可以调用
				  else if(status==2){
					 int  adapt=CronParserUtil.adaptTask(task,time);
					 if(adapt==1){
						  status=1;
					 }
				  }
				  if(status==1){
					 //执行调度，设置状态
					  SimpleDateFormat s=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					  String next=s.format(task.getCurrentFiredTime());
					  logger.error(next+"-- fire time"+task.getPath());
					// System.out.println(next+"-- fire time"+task.getPath());
				  }
				zkClient.writeData(task.getGroup()+"/"+task.getPath(), task);
			  }
		  }
	  }
	}
	
	public static void main(String[] args) {
		ScheduledExecutorService scheduExec = Executors.newScheduledThreadPool(1);
		scheduExec.scheduleAtFixedRate(new TaskScanner(), 0, ConfigUtil.getInt("scan.period"), TimeUnit.MILLISECONDS);
	}
}
