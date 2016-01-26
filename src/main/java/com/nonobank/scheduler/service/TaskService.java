package com.nonobank.scheduler.service;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.nonobank.scheduler.entity.SchedulerTask;
import com.nonobank.scheduler.util.ConfigUtil;


public class TaskService{

	private static  String HOST=ConfigUtil.getString("zookeeper.host");
	
	private static  int CLIENT_SESSION_TIMEOUT=ConfigUtil.getInt("zookeeper.session.timeout");
	
	private static  String ROOT_TASK=ConfigUtil.getString("zookeeper.root.taskpath");
	
	private static  String ROOT_SCHEDULERS=ConfigUtil.getString("zookeeper.root.scheduler");
	
	private static String path=null;
	
	private static Logger logger = Logger.getLogger(TaskService.class);
	public TaskService(){
		
	}

	/**
	 * 新增任务
	 * @param task
	 * @throws IOException
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void addTask(SchedulerTask task) throws IOException, KeeperException, InterruptedException{
		ZkClient zkClient=new ZkClient(HOST,CLIENT_SESSION_TIMEOUT);
		if(!zkClient.exists(ROOT_TASK)){
			zkClient.createPersistent(ROOT_TASK);
		}
		path=zkClient.createPersistentSequential(ROOT_TASK+"/"+"task-", task);
		zkClient.close();
		logger.info("created task:"+path);
	}
	
	
	/**
	 * 更新任务
	 * @param task
	 * @return
	 * @throws IOException
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static int updateTask(SchedulerTask task) throws IOException, KeeperException, InterruptedException{
		ZkClient zkClient=new ZkClient(HOST,CLIENT_SESSION_TIMEOUT);
		//创建持久节点
		if(!zkClient.exists(ROOT_TASK)){
			zkClient.createPersistent(ROOT_TASK);
		}
		task.setGroup(ROOT_TASK);
		System.out.println(task.getGroup()+"/"+task.getPath());
	    if(zkClient.exists(task.getGroup()+"/"+task.getPath())){
	      zkClient.writeData(task.getGroup()+"/"+task.getPath(), task);
	      zkClient.close();
	      logger.info("update tasks:"+task.getName());
	      return 0;
	    }
	    else{
	    	zkClient.close();
	    	return -1;
	    }
	}
	
	/**
	 * 删除任务
	 */
	public static int deleteTask(String taskName){
		ZkClient zkClient=new ZkClient(HOST,CLIENT_SESSION_TIMEOUT);
		//创建持久节点
		if(!zkClient.exists(ROOT_TASK)||taskName==null){
			zkClient.createPersistent(ROOT_TASK);
			return -1;
		}
		List<String> children=zkClient.getChildren(ROOT_TASK);
		if(children!=null && children.size()>0){
			for(int i=0;i<children.size();i++){
		    String path=ROOT_TASK+"/"+children.get(i);
			SchedulerTask t=zkClient.readData(path);
			if(t.getName().equals(taskName)){
				if(zkClient.delete(path)){
					logger.info("delete task:"+taskName);
					return 0;
				}
			}
			}
		}
		return -1;
	}
	/**
	 * 获取当前活跃任务列表
	 */
	public static List<SchedulerTask> getAliveTaskList(){
		ZkClient zkClient=new ZkClient(HOST,CLIENT_SESSION_TIMEOUT);
		List<SchedulerTask> result=new ArrayList<>();
		if(!zkClient.exists(ROOT_TASK)){
			zkClient.createPersistent(ROOT_TASK);
			return result;
		}
		List<String> children=zkClient.getChildren(ROOT_TASK);
		if(children!=null && children.size()>0){
			for(int i=0;i<children.size();i++){
			SchedulerTask t=zkClient.readData(ROOT_TASK+"/"+children.get(i));
			result.add(t);
			}
		}
		zkClient.close();
		return result;
	}
	
	/**
	 * 获取当前活跃调度机器列表
	 */
	public static List<String> getAliveSchedulers(){
		ZkClient zkClient=new ZkClient(HOST,CLIENT_SESSION_TIMEOUT);
		List<String> result=new ArrayList<>();
		if(!zkClient.exists(ROOT_SCHEDULERS)){
			zkClient.createPersistent(ROOT_SCHEDULERS);
			return result;
		}
		result=zkClient.getChildren(ROOT_SCHEDULERS);
		return result;
	}
	

	
	
}
