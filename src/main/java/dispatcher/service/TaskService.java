package dispatcher.service;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import dispatcher.entity.SchedulerTask;
import dispatcher.util.ConfigUtil;

public class TaskService{

	public static  String HOST=ConfigUtil.getString("zookeeper.host");
	
	public static  int CLIENT_SESSION_TIMEOUT=ConfigUtil.getInt("zookeeper.session.timeout");
	
	public static  String ROOT_TASK=ConfigUtil.getString("zookeeper.root.taskpath");
	
	public static  String ROOT_SCHEDULERS=ConfigUtil.getString("zookeeper.root.scheduler");
	
	private static String path=null;
	
	private static Logger logger = Logger.getLogger(TaskService.class);
	public TaskService(){
	
	}
	
	/**
	 * 新增任务
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
	public static int deleteTask(SchedulerTask task){
		ZkClient zkClient=new ZkClient(HOST,CLIENT_SESSION_TIMEOUT);
		//创建持久节点
		if(!zkClient.exists(ROOT_TASK)||task==null||task.getName()==null){
			zkClient.createPersistent(ROOT_TASK);
			return -1;
		}
		List<String> children=zkClient.getChildren(ROOT_TASK);
		if(children!=null && children.size()>0){
			for(int i=0;i<children.size();i++){
		    String path=ROOT_TASK+"/"+children.get(i);
			SchedulerTask t=zkClient.readData(path);
			if(t.getName().equals(task.getName())){
				if(zkClient.delete(path)){
					logger.info("delete task:"+task.getName());
					return 0;
				}
			}
			}
		}
		return -1;
	}
	
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
		return result;
	}
	
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
	
	
	public static void testAdd(String[] args) throws Exception{
		
		Calendar c=Calendar.getInstance();
		c.set(Calendar.MINUTE, c.get(Calendar.MINUTE)-1);
		c.set(Calendar.SECOND, 0);
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //设置时间格式
		String defaultStartDate = sdf.format(c.getTime()); 
		SchedulerTask task2=new SchedulerTask(1, "task demo2","group","path",
				"url2", "*/10 * * * * *", 0,c.getTime(),new Date(), "describe", new Date(),
				new Date());
		System.out.println(defaultStartDate+"-start time-");
		TaskService.addTask(task2);
	//	TaskService.updateTask("/tasks/task-0000000004",task2);
	}
	
	public static void main(String[] args) {
		
		Calendar c=Calendar.getInstance();
		c.set(Calendar.MINUTE, c.get(Calendar.MINUTE)-1);
		c.set(Calendar.SECOND, 0);
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //设置时间格式
		String defaultStartDate = sdf.format(c.getTime()); 
		SchedulerTask task2=new SchedulerTask(1, "task demo2","/tasks","task-0000000000",
				"url2", "*/10 * * * * *", 0,c.getTime(),new Date(), "describe", new Date(),
				new Date());
		System.out.println(defaultStartDate+"-start time-");
		try {
			System.out.println(task2.getPath());
			
			TaskService.updateTask(task2);
		} catch (Exception e) {
			e.printStackTrace();
		}
	//	TaskService.updateTask("/tasks/task-0000000004",task2);
	}
	
	
}
