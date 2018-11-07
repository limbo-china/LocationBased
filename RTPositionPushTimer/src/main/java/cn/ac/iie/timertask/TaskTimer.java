package cn.ac.iie.timertask;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public class TaskTimer {
	Timer timer;

	public TaskTimer() {
		Date time = getTime();
		timer = new Timer();
		timer.schedule(new TimerTaskTest02(), time, 1000);
	}

	public Date getTime() {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, 1);
		calendar.set(Calendar.MINUTE, 03);
		calendar.set(Calendar.SECOND, 00);
		Date time = calendar.getTime();

		return time;
	}

	public static void main(String[] args) {
		new TaskTimer();
	}

	public class TimerTaskTest02 extends TimerTask {

		@Override
		public void run() {
			System.out.println("指定时间执行线程任务...");
		}
	}
}
