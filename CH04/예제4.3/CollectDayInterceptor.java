package com.wikibook.bigdata.smartcar.flume;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;


public class CollectDayInterceptor implements Interceptor {


	public CollectDayInterceptor(){
	}

	@Override
	public void initialize() {

	}

	@Override
	public Event intercept(Event event) {

		String eventBody = new String(event.getBody()) + "," + getToDate();
		event.setBody(eventBody.getBytes());
		return event;

	}


	@Override
	public void close() {
	}

	
	@Override
	public List<Event> intercept(List<Event> events)
	{
		for (Event event:events) {
			intercept(event);
		}
		return events;
	}
	

	public static class Builder implements Interceptor.Builder
	{
		@Override
		public void configure(Context context) {
		}

		@Override
		public Interceptor build() {
			return new CollectDayInterceptor();
		}
	}

	public  String getToDate() {

		long todaytime;
		SimpleDateFormat day;
		String toDay;

		todaytime = System.currentTimeMillis(); 
		day = new SimpleDateFormat("yyyyMMdd");

		toDay =  day.format(new Date(todaytime));

		return toDay;

	}
}