package com.nonobank.scheduler.service;


import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.hash.Hashing;
import com.nonobank.scheduler.entity.SchedulerTask;
import com.nonobank.scheduler.listener.DataListener;
import com.nonobank.scheduler.listener.SchedulerListener;
import com.nonobank.scheduler.listener.TaskListener;
import com.nonobank.scheduler.util.ConfigUtil;

/**
 * 任务调度主要功能类
 * @author geyingchao
 *
 */
public class SchedulerService {
	
	private static SchedulerService zkService;
	
	public static ZkClient zkClient;
	
	public static List<SchedulerTask> schedulerTaskList= Collections.synchronizedList(new ArrayList<SchedulerTask>());
	
	private static  String HOST=ConfigUtil.getString("zookeeper.host");
	
	private static  int CLIENT_SESSION_TIMEOUT=ConfigUtil.getInt("zookeeper.session.timeout");
	
	private static  String ROOT_SCHEDULER=ConfigUtil.getString("zookeeper.root.scheduler");
	
	private static  String ROOT_TASK=ConfigUtil.getString("zookeeper.root.taskpath");
	
	private static String path;
	
	private static Logger logger = Logger.getLogger(SchedulerService.class);
	
	private SchedulerService(){
		try {
			init();
		} catch (Exception e) {
			e.printStackTrace();
	    	logger.error("connected to zookeeper error!");
		}
	}
	
	
	/**
	 * 获取service对象
	 * @return
	 */
	public static SchedulerService newInstance(){
		   if(zkService==null){
			   return new SchedulerService();
		   }
		   else{
			   return zkService;
		   }
		}
	
	/**
	 * 初始化service，tasklist
	 * @throws Exception
	 */
	private void init()throws Exception{
		
		zkClient=new ZkClient(HOST,CLIENT_SESSION_TIMEOUT);
		//创建持久节点
		if(!zkClient.exists(ROOT_SCHEDULER)){
			zkClient.createPersistent(ROOT_SCHEDULER);
		}
		if(!zkClient.exists(ROOT_TASK)){
			zkClient.createPersistent(ROOT_TASK);
		}else{
			List<String> children=zkClient.getChildren(ROOT_TASK);
			for(int i=0;i<children.size();i++){
				zkClient.subscribeDataChanges(ROOT_TASK+"/"+children.get(i), new DataListener(this));
			}
		}
		zkClient.subscribeChildChanges(ROOT_TASK,new TaskListener(this));
		//创建临时节点
		String ip=null;
		try{
			ip=InetAddress.getLocalHost().getHostAddress();
		}catch(Exception e){
		}
		path=zkClient.createEphemeralSequential(ROOT_SCHEDULER+"/scheduler-", ip);
		zkClient.subscribeChildChanges(ROOT_SCHEDULER, new SchedulerListener(this));
		getSchedulerTaskList();
		logger.info(path+" scheduler start");
		
	}
	
	
	/**
	 * 获取该调度服务器任务列表，通过一致性hash算法确定是否调度该任务
	 * @param zk  ZkClient 对象
	 * @return  任务列表list
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public  List<SchedulerTask> getSchedulerTaskList() throws KeeperException, InterruptedException {
		schedulerTaskList.clear();
		ArrayList<SchedulerTask> tasks=getTasksList();
		ArrayList<Long> schedulers=getSchedulersList();
		//添加任务到节点列表
		for(int i=0;i<tasks.size();i++){
			SchedulerTask task=tasks.get(i);
			long taskid=getSeq(task.getPath());
			int buckets=schedulers.size();
			int group=Hashing.consistentHash(taskid, buckets);
			long order=getOrder(getSeq(path),schedulers);
			if(group==order){
				schedulerTaskList.add(task);	
			}
		}
		return schedulerTaskList;
	}
	
	/**
	 * 根据有序节点编号获取节点号
	 * @param path
	 * @return
	 */
	private long getSeq(String path){
		return Long.parseLong(path.split("-")[1]);
	}
	
	/**
	 * 获取scheduler节点在所有任务节点的排位（从小到大）
	 * @param value  该节点在所有节点中的序号
	 * @param total
	 * @return
	 */
	private long getOrder(long value,ArrayList<Long> total){
		 Collections.sort(total);
		 int result=-1;
		if(total!=null && total.size()>0){
			for(int i=0;i<total.size();i++){
		    	if(total.get(i)==value)
		    	{
		    		result=i;
			    	break;
		    	}
			}
		}
		return result;
	}
	
	    /**
	     * 获取调度服务器列表
	     * @param zk
	     * @return
	     * @throws KeeperException
	     * @throws InterruptedException
	     */
		private ArrayList<Long> getSchedulersList() throws KeeperException, InterruptedException {
			ArrayList<Long> schedulers=new ArrayList<>();
			List<String> children= zkClient.getChildren(ROOT_SCHEDULER);
		    if(children!=null && children.size()>0)
		    for(int i=0;i<children.size();i++){
			schedulers.add(getSeq(children.get(i)));
		   }
			return schedulers;
		}
		
		
		/**
		 * 获取所有任务列表
		 * @param zk
		 * @return
		 * @throws KeeperException
		 * @throws InterruptedException
		 */
		private ArrayList<SchedulerTask> getTasksList() throws KeeperException, InterruptedException {
			ArrayList<SchedulerTask> tasks=new ArrayList<>();
			List<String> children= zkClient.getChildren(ROOT_TASK);
	    	if(children!=null && children.size()>0)
	    	for(int i=0;i<children.size();i++){
			//获取每一个节点的任务信息
			 SchedulerTask task=zkClient.readData(ROOT_TASK+"/"+children.get(i));
			//将节点路径也存放到SchedulerTask中，后面分任务时使用
			task.setPath(children.get(i));
			task.setGroup(ROOT_TASK);
			tasks.add(task);
		}
			return tasks;
		}
		
}
