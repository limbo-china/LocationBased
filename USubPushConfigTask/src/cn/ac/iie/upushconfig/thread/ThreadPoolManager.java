package cn.ac.iie.upushconfig.thread;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**

 */
public final class ThreadPoolManager {

	private static ThreadPoolManager sThreadPoolManager = new ThreadPoolManager();

	// 线程池维护线程的�?少数�?
	private static final int SIZE_CORE_POOL = 50;

	// 线程池维护线程的�?大数�?
	private static final int SIZE_MAX_POOL = 100;

	// 线程池维护线程所允许的空闲时�?
	private static final int TIME_KEEP_ALIVE = 5000;

	// 线程池所使用的缓冲队列大�?
	private static final int SIZE_WORK_QUEUE = 500;

	// 任务调度周期
	private static final int PERIOD_TASK_QOS = 1000;

	/*
	 * 线程池单例创建方�?
	 */
	public static ThreadPoolManager newInstance() {
		return sThreadPoolManager;
	}

	// 任务缓冲队列
	private final Queue<Runnable> mTaskQueue = new LinkedList<Runnable>();

	/*
	 * 线程池超出界线时将任务加入缓冲队�?
	 */
	private final RejectedExecutionHandler mHandler = new RejectedExecutionHandler() {
		@Override
		public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
			mTaskQueue.offer(task);
		}
	};

	/*
	 * 将缓冲队列中的任务重新加载到线程�?
	 */
	private final Runnable mAccessBufferThread = new Runnable() {
		@Override
		public void run() {
			if (hasMoreAcquire()) {
				mThreadPool.execute(mTaskQueue.poll());
			}
		}
	};

	/*
	 * 创建�?个调度线程池
	 */
	private final ScheduledExecutorService scheduler = Executors
			.newScheduledThreadPool(1);

	/*
	 * 通过调度线程周期性的执行缓冲队列中任�?
	 */
	protected final ScheduledFuture<?> mTaskHandler = scheduler
			.scheduleAtFixedRate(mAccessBufferThread, 0, PERIOD_TASK_QOS,
					TimeUnit.MILLISECONDS);

	/*
	 * 线程�?
	 */
	private final ThreadPoolExecutor mThreadPool = new ThreadPoolExecutor(
			SIZE_CORE_POOL, SIZE_MAX_POOL, TIME_KEEP_ALIVE, TimeUnit.SECONDS,
			new ArrayBlockingQueue<Runnable>(SIZE_WORK_QUEUE), mHandler);

	/*
	 * 将构造方法访问修饰符设为私有，禁止任意实例化�?
	 */
	private ThreadPoolManager() {
	}

	public void perpare() {
		if (mThreadPool.isShutdown() && !mThreadPool.prestartCoreThread()) {
			@SuppressWarnings("unused")
			int startThread = mThreadPool.prestartAllCoreThreads();
		}
	}

	/*
	 * 消息队列�?查方�?
	 */
	private boolean hasMoreAcquire() {
		return !mTaskQueue.isEmpty();
	}

	/*
	 * 向线程池中添加任务方�?
	 */
	public void addExecuteTask(Runnable task) {
		if (task != null) {
			mThreadPool.execute(task);
		}
	}

	protected boolean isTaskEnd() {
		if (mThreadPool.getActiveCount() == 0) {
			return true;
		} else {
			return false;
		}
	}

	public void shutdown() {
		mTaskQueue.clear();
		mThreadPool.shutdown();
	}
}
