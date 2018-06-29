package cn.ac.iie.hy.datatrans.server;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by siyu on 2017/5/23.
 */
public class KafkaMultiProcessor {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMultiProcessor.class);
    //订阅的topic
    private String alarmTopic;

    private Properties propsConsumer = new Properties();

    private Thread[] threads;

    public static void main(String[] args) throws IOException {
        KafkaMultiProcessor kafka = new KafkaMultiProcessor();
        
        kafka.init("consumerConfig.properties");
		
        
    }

    public void init(String confPathConsumer) throws IOException {

        long startTime = System.currentTimeMillis();

        //Properties props = new Properties();
        InputStream configInputStream = new FileInputStream(new File(confPathConsumer));
        propsConsumer.load(configInputStream);


        alarmTopic = propsConsumer.getProperty("topic.name");

        logger.info("=============="+"get kafka consumerConfig: " + propsConsumer.toString()+"==============");

        //创建threadsNum个线程用于读取kafka消息
        //threadsNum的个数即为consumer的个�?
        int threadsNum = Integer.parseInt(propsConsumer.getProperty("consumerNum"));
        logger.info("=============="+"create " + threadsNum + " threads to consume kafka warn msg"+"==============");

        threads = new Thread[threadsNum];
        for (int i = 0; i < threadsNum; i++) {
            //MsgReceiver msgReceiver = new MsgReceiver(propsConsumer,propsProducer, alarmTopic, recordProcessorTasks, recordProcessorThreads);
            MsgReceiver msgReceiver = new MsgReceiver(propsConsumer, alarmTopic);
            Thread thread = new Thread(msgReceiver);
            threads[i] = thread;
            thread.setName("topic consumer " + i);
        }
        //启动这几个线�?
        for (int i = 0; i < threadsNum; i++) {
            threads[i].start();
        }

        logger.info("=============="+"finish creating" + threadsNum + " threads to consume kafka warn msg"+"==============");
    }

    //�?��启动的线�?
    public void destroy() {
        //closeRecordProcessThreads();
        closeKafkaConsumer();
    }

    /*private void closeRecordProcessThreads() {
        logger.debug("start to interrupt record process threads");
        for (Map.Entry<TopicPartition, Thread> entry : recordProcessorThreads.entrySet()) {
            Thread thread = entry.getValue();
            thread.interrupt();
        }
        logger.debug("finish interrupting record process threads");
    }*/

    private void closeKafkaConsumer() {
        logger.debug("start to interrupt kafka consumer threads");
        //使用interrupt中断线程, 在线程的执行方法中已经设置了响应中断信号
        for (int i = 0; i < threads.length; i++) {
            threads[i].interrupt();
        }
        logger.debug("finish interrupting consumer threads");
    }


}