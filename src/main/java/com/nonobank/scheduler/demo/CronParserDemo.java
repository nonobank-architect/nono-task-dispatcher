package com.nonobank.scheduler.demo;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.cronutils.descriptor.CronDescriptor;
import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;

public class CronParserDemo {

	public static void test1(String[] args) { 
		DateTime now = DateTime.now();
		CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ);
		CronParser parser = new CronParser(cronDefinition);
		Cron quartzCron = parser.parse("* */1 * * * *");
		ExecutionTime executionTime = ExecutionTime.forCron(quartzCron);
		DateTime nextExecution = executionTime.nextExecution(now);
		Date date=nextExecution.toDate();
		CronDescriptor descriptor = CronDescriptor.instance(Locale.UK);
		String description = descriptor.describe(parser.parse("* */1 * * * *"));
		System.out.println(description);
		Calendar c=Calendar.getInstance();
		c.set(Calendar.MINUTE, c.get(Calendar.MINUTE)-5);
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //设置时间格式
		
		String defaultStartDate = sdf.format(c.getTime()); 
		System.out.println(defaultStartDate+"--开始时间---");
		
		DateTime lastExecution = executionTime.lastExecution(new DateTime(c));
		System.out.println(sdf.format(lastExecution.toDate())+"---最近一次--");

	}
	
	public static void main(String[] args) {
		Calendar c=Calendar.getInstance();
		c.set(Calendar.MINUTE, c.get(Calendar.MINUTE)-5);
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //设置时间格式
		
		String defaultStartDate = sdf.format(c.getTime()); 
		System.out.println(defaultStartDate+"--开始时间---");
		
		CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ);
		CronParser parser = new CronParser(cronDefinition);
		Cron quartzCron = parser.parse("* */1 * * * *");
		ExecutionTime executionTime = ExecutionTime.forCron(quartzCron);
		DateTime lastExecution = executionTime.nextExecution(DateTime.now());
		System.out.println(sdf.format(lastExecution.toDate())+"---最近一次--");
		
	}
	
}
