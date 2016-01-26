package com.nonobank.scheduler.demo;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.nonobank.scheduler.entity.SchedulerTask;
import com.nonobank.scheduler.service.TaskService;

public class TaskServiceTest {

	
	public static void testAdd() throws Exception{
		
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
	}
	
	public static void testUpdate(String path) {
		Calendar c=Calendar.getInstance();
		c.set(Calendar.MINUTE, c.get(Calendar.MINUTE));
		c.set(Calendar.SECOND, 0);
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //设置时间格式
		String defaultStartDate = sdf.format(c.getTime()); 
		SchedulerTask task2=new SchedulerTask(1, "task demo2","/tasks",path,
				"url2", "*/10 * * * * *", 0,c.getTime(),new Date(), "describe", new Date(),
				new Date());
		System.out.println(defaultStartDate+"-start time-");
		try {
			TaskService.updateTask(task2);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void testDelete(String path){
		TaskService.deleteTask(path);
	}
	public static void testAliveTaskList(){
		List<SchedulerTask> list=TaskService.getAliveTaskList();
		for(int i=0;i<list.size();i++){
			System.out.println(list.get(i).getName());
		}
	}
	
	public static void testAliveSchedulers(){
		List<String> list=TaskService.getAliveSchedulers();
		for(int i=0;i<list.size();i++){
			System.out.println(list.get(i));
		}
	}
	
	public static void main(String[] args) {
		try {
//			testAdd();
//			testUpdate("task-0000000001");
			//testDelete("task demo2");
			//testAliveTaskList();
//			testAliveSchedulers();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
