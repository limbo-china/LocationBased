package cn.ac.iie.datadispatch.rabbit;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;



public class DataFactory {

	private static BlockingQueue queue;


	public DataFactory(int maxSize) {
		queue = new LinkedBlockingQueue(maxSize);

	}


	/**
	 * 可以设定等待的时间，如果在指定的时间内，还不能往队列中加入BlockingQueue，则返回失败。
	 */
	public boolean sendData(List data,long timeout, TimeUnit unit) {
		try {
			queue.offer(data, timeout, unit);
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * 表示如果可能的话,将anObject加到BlockingQueue里,即如果BlockingQueue可以容纳,则返回true,否则返回false.（本方法不阻塞当前执行方法的线程）
	 */
	public boolean sendData(List data) throws InterruptedException {
		return queue.offer(data);
	}
	
	/**
	 * 把anObject加到BlockingQueue里,如果BlockQueue没有空间,则调用此方法的线程被阻断,直到BlockingQueue里面有空间再继续.
	 */	
	public void putData(List data) throws InterruptedException {
		 queue.put(data);
	}
	

	
	
	/**
	 *从BlockingQueue取出一个队首的对象，如果在指定时间内，　队列一旦有数据可取，则立即返回队列中的数据。否则知道时间超时还没有数据可取，返回失败。
	 */		
	public Object getData(long timeout, TimeUnit timeUnit) {
		try {
			return queue.poll(timeout, timeUnit);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "null";

		}
	}
	
	/**
	 *取走BlockingQueue里排在首位的对象,若BlockingQueue为空,阻断进入等待状态直到BlockingQueue有新的数据被加入; 
	 */	
	public Object take() {
		try {
			return queue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "null";

		}
	}
	
	/**
	 *Removes all available elements from this queue and adds them to the given collection. 
	 *This operation may be more efficient than repeatedly polling this queue. A failure 
	 *encountered while attempting to add elements to collection c may result in elements
	 * being in neither, either or both collections when the associated exception is thrown. 
	 * Attempts to drain a queue to itself result in IllegalArgumentException. Further, 
	 * the behavior of this operation is undefined if the specified collection is 
	 * modified while the operation is in progress.
	 */	
	@SuppressWarnings("unchecked")
	public Object drainTo(Collection c){
		return queue.drainTo(c);
	}
	
	
	/**
	 *	Removes at most the given number of available elements from this queue and adds them to 
	 *the given collection. A failure encountered while attempting to add elements to collection 
	 *c may result in elements being in neither, either or both collections when the associated 
	 *exception is thrown. Attempts to drain a queue to itself result in IllegalArgumentException.
	 * Further, the behavior of this operation is undefined if the specified collection is 
	 * modified while the operation is in progress.
	 */	
	@SuppressWarnings("unchecked")
	public int drainTo(Collection c,int maxElements){
		return queue.drainTo(c, maxElements);
	}
	
}
