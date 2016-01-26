package dispatcher.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;

import dispatcher.entity.SchedulerTask;

public class CronParserUtil {
	
	private static Logger logger = Logger.getLogger(CronParserUtil.class);
	/**
	 * 判断任务是否可以被调度和是否需要调整
	 * @param task
	 * @param time
	 * @return 0：表示调度周期未到；1：表示可以被调度；2：表示可以被调整；
	 */
	public static int isExecutable(SchedulerTask task,Date time){
		if(task!=null){
			if(task.getCurrentFiredTime()==null){
			   task.setCurrentFiredTime(time);
			}
			long current=task.getCurrentFiredTime().getTime();  
		    long cycle=getCycle(task);
		    long now=time.getTime();
		    long interval=current-now;    //时间差
          //时间差在(-period<=interval<=period) 成功调度
		    if((Math.abs(interval)<=ConfigUtil.getInt("scan.period")))
		    {
		    	return 1; 
		    } //超过一个时间周期需要调整执行时间
		    else if(interval<=(-cycle)){
		      return 2;
		    }
		}
		return 0;
	}
	/**
	 * 当前调度时间根据上次调度时间计算
	 * 下次调度时间根据当前调度时间计算
	 */
	public static void updateTask(SchedulerTask task){
		String cron=task.getCron();
		CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ);
	    CronParser parser = new CronParser(cronDefinition);
		Cron quartzCron = parser.parse(cron);
		ExecutionTime executionTime = ExecutionTime.forCron(quartzCron);
		DateTime updateCurrentFireTime = executionTime.nextExecution(new DateTime(task.getCurrentFiredTime()));
		//更新当前调度时间
		task.setCurrentFiredTime(updateCurrentFireTime.toDate());
		DateTime nextFireTime = executionTime.nextExecution(updateCurrentFireTime);
		//更新下次调度时间
		task.setNextFiredTime(nextFireTime.toDate());
	}
	
	/***
	 * 将任务调度时间调整为以time开始的，最近调度时间
	 * @param task  被调度的任务
	 * @param time  待调整起始时间
	 * @return
	 */
	public static int adaptTask(SchedulerTask task,Date time){
		String cron=task.getCron();
		CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ);
	    CronParser parser = new CronParser(cronDefinition);
		Cron quartzCron = parser.parse(cron);
		ExecutionTime executionTime = ExecutionTime.forCron(quartzCron);
		DateTime updateCurrentFireTime=new DateTime(time);
	    updateCurrentFireTime = executionTime.nextExecution(new DateTime());
		//更新当前调度时间
		task.setCurrentFiredTime(updateCurrentFireTime.toDate());
		DateTime nextFireTime = executionTime.nextExecution(updateCurrentFireTime);
		//更新下次调度时间
		task.setNextFiredTime(nextFireTime.toDate());
		int result=isExecutable(task,time);
		return result;
	}
	
	/**
	 * 获取调度任务的调度周期
	 * 
	 */
	public static long getCycle(SchedulerTask task){
		long cycle=0l;
		DateTime start;
		DateTime end;
		if(task!=null && task.getCron()!=null ){
			String cron=task.getCron();
			CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ);
		    CronParser parser = new CronParser(cronDefinition);
			Cron quartzCron = parser.parse(cron);
			ExecutionTime executionTime = ExecutionTime.forCron(quartzCron);
			start = executionTime.nextExecution(new DateTime());
			end = executionTime.nextExecution(start);
			cycle=end.toDate().getTime()-start.toDate().getTime();
		}
		return cycle;
	}

	
	public static void main(String[] args) {
		
		String cron="*/15 * * * * *";
		Calendar c=Calendar.getInstance();
		c.set(Calendar.MINUTE, c.get(Calendar.MINUTE)+5);
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //设置时间格式
		String defaultStartDate = sdf.format(c.getTime()); 
		Date now=c.getTime();
		System.out.println(defaultStartDate);
		CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ);
	    CronParser parser = new CronParser(cronDefinition);
		Cron quartzCron = parser.parse(cron);
		
		ExecutionTime executionTime = ExecutionTime.forCron(quartzCron);
		
		for(int i=0;i<100;i++){
			now = executionTime.nextExecution(new DateTime(now)).toDate();
			System.out.println(sdf.format(now));
		}
	}
	
}
