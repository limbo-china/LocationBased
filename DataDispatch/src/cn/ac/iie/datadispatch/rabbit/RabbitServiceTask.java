package cn.ac.iie.datadispatch.rabbit;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;


public class RabbitServiceTask implements Runnable {

	private boolean isRunning = false;
	private DataFactory df = null;
	static Logger logger = null;
	private String rabbitHost = null;
	private int rabbitPort = -1;
	private String rabbitUserName = null;
	private String rabbitUserPasswd = null;
	private String exChangeName = null;
	private String directKey = null;
	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(RabbitServiceTask.class.getName());
	}
	public RabbitServiceTask(boolean isRunning, DataFactory df) {
		super();
		this.isRunning = isRunning;
		this.df  = df;
		Properties pps = new Properties();
		try {
			InputStream in = new BufferedInputStream(new FileInputStream("data-dispatcher.properties"));
			pps.load(in);
		} catch (Exception e) {
			e.printStackTrace();
		}
		rabbitHost = pps.getProperty("rabbitHost");
		rabbitPort = Integer.parseInt(pps.getProperty("rabbitPort"));
		rabbitUserName = pps.getProperty("rabbitUserName");
		rabbitUserPasswd = pps.getProperty("rabbitUserPasswd");
		exChangeName = pps.getProperty("exChangeName");
		directKey = pps.getProperty("directKey");
	}

	@Override
	public void run() {
		isRunning = true;
		Gson gson = new Gson();
		while(isRunning){
			List al = (List)df.take();
			//logger.info("rabbit" + gson.toJson(al));
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(rabbitHost);
			factory.setPort(rabbitPort);
			factory.setUsername(rabbitUserName);
			factory.setPassword(rabbitUserPasswd);
			Connection connection = null;
			Channel channel = null;
			try {
				connection = factory.newConnection();
				channel = connection.createChannel();

				// channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

				channel.exchangeDeclare(exChangeName, "direct");

				// String message = getMessage(argv);
				String message = "{\"Result\":" +  gson.toJson(al) + "}";
				channel.basicPublish(exChangeName, directKey, null, message.getBytes());
				//System.out.println(" [x] Producer : sent '" + message + "'");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally{
				try {
					if(channel != null){
						channel.close();

					}
					if(connection != null){
						connection.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				
			}
			

			
			
		}
	}

}
