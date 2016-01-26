package com.nonobank.scheduler.demo;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKClientDemo{

	public static  String HOST=null;
	
	public static  int CLIENT_SESSION_TIMEOUT=200;
	
	public static  String LEADER_PATH="/smsm";
	
	private String path=null;
	
	public static boolean lock=false;
	
	private  final Logger logger = LoggerFactory.getLogger(getClass());
	
	/**
	 * 
	 * @param ZKHost  zookeeper的ip地址和端口号，多台机器只�?要一个ip地址就行
	 * @param taskName 本次定时任务调度的任务名，必须有
	 */
	public ZKClientDemo(String ZKHost,String taskName){
		this.HOST=ZKHost;
		this.LEADER_PATH=taskName;
	}
	
	public void run()throws Exception{
		
		ZooKeeper zk=new ZooKeeper(HOST,CLIENT_SESSION_TIMEOUT,new Watcher(){
			public void process(WatchedEvent event) {
				logger.info("---watcher--eventtype=="+event.getType());
			}
		});
	
		//创建持久节点
		if(zk.exists(LEADER_PATH,false)==null){
			zk.create(LEADER_PATH, null,Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		//创建临时节点
		path=zk.create(LEADER_PATH+"/lock-", null,Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		
		logger.info(path);
		
		getLock(zk);
	}
	
	private void getLock(final  ZooKeeper zk) throws KeeperException, InterruptedException  {
		List<String> children= zk.getChildren(LEADER_PATH, new Watcher(){
			public void process(WatchedEvent event) {
				if(event.getType().equals(EventType.NodeChildrenChanged)){
					try{
						getLock(zk);
					}catch(KeeperException e) { 
						e.printStackTrace(); 
				     }catch(InterruptedException e) { 
					    e.printStackTrace(); 
					 } 
				}
			}
		});
	//获取自己的序号和�?有序号比较，取最小序号为�?
	long seq=getSeq(path);
	if(children==null||children.size()==1){
		lock=true;
	}else{
		long min=getMinSeq(children);
		if(min==seq){
			lock=true;
		}
	}
}
	
	private long getSeq(String path){
		return Long.parseLong(path.split("-")[1]);
	}
	
	private long getMinSeq(List<String> seq){
		long temp=getSeq(seq.get(0));
		for(int i=0;i<seq.size();i++){
			long child=getSeq(seq.get(i));
			if(temp >=child){
				temp=child;
			}
		}
		return temp;
	}
	
}
