package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.trigger.XxlJobTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * job trigger thread pool helper
 *
 * @author xuxueli 2018-07-03 21:08:07
 */
public class JobTriggerPoolHelper {
    private static Logger logger = LoggerFactory.getLogger(JobTriggerPoolHelper.class);


    // ---------------------- trigger pool ----------------------

    // todo 快线程池
    private ThreadPoolExecutor fastTriggerPool = null;
    // todo 慢线程池
    private ThreadPoolExecutor slowTriggerPool = null;

    public void start(){
        //todo 最大200线程，最多处理1000任务
        fastTriggerPool = new ThreadPoolExecutor(
                10,
                // 最大线程数
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(1000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "xxl-job, admin JobTriggerPoolHelper-fastTriggerPool-" + r.hashCode());
                    }
                });

        //todo 最大100线程，最多处理2000任务
        // 一分钟内容超时超过10次，则采用慢触发器执行
        slowTriggerPool = new ThreadPoolExecutor(
                10,
                // 最大线程数
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "xxl-job, admin JobTriggerPoolHelper-slowTriggerPool-" + r.hashCode());
                    }
                });
    }

    // 停止当前线程池
    public void stop() {
        //todo 分析一下停止线程池的写法
        fastTriggerPool.shutdownNow();
        slowTriggerPool.shutdownNow();
        logger.info(">>>>>>>>> xxl-job trigger thread pool shutdown success.");
    }


    // job timeout count
    private volatile long minTim = System.currentTimeMillis()/60000;     // ms > min
    // todo 存放每个任务的执行慢次数，60秒后自动清空该容器, key 是任务id   value 是  次数
    private volatile ConcurrentMap<Integer, AtomicInteger> jobTimeoutCountMap = new ConcurrentHashMap<>();


    /**
     * todo 主动触发一次任务
     * todo 在界面中，点击任务的执行，会触发一次任务，后台会调用JobTriggerPoolHelper.trigger() 任务。
     * todo 该方法会将任务给提交一个线程池，在线程池中调用XXlJobTrigger.trigger
     *
     * add trigger 新增 触发器
     * 执行任务时，首先判断这个任务是否是个慢任务，
     * 如果是个慢任务且慢执行的次数超过了10次将会使用slowTriggerPool慢线程池，
     * 它的统计周期为60秒，这里是个优化点，当有大量的任务被执行时，
     * 为了防止任务被阻塞，尽可能的会先让执行快的任务优先执行。
     */
    public void addTrigger(final int jobId,
                           final TriggerTypeEnum triggerType,
                           final int failRetryCount,
                           final String executorShardingParam,
                           final String executorParam,
                           final String addressList) {

        // choose thread pool
        ThreadPoolExecutor triggerPool_ = fastTriggerPool;
        AtomicInteger jobTimeoutCount = jobTimeoutCountMap.get(jobId);
        if (jobTimeoutCount!=null && jobTimeoutCount.get() > 10) {      // job-timeout 10 times in 1 min
            triggerPool_ = slowTriggerPool;
        }

        // todo 执行触发器线程 线程池执行
        triggerPool_.execute(new Runnable() {
            @Override
            public void run() {

                long start = System.currentTimeMillis();

                try {
                    // todo 开始任务触发
                    XxlJobTrigger.trigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                } finally {

                    // check timeout-count-map
                    long minTim_now = System.currentTimeMillis()/60000;
                    if (minTim != minTim_now) {
                        minTim = minTim_now;
                        jobTimeoutCountMap.clear();
                    }

                    // todo 默认先使用快线程池，但当一分钟以内任务超过10次执行时间超过500ms，则加入慢线程池执行
                    long cost = System.currentTimeMillis()-start;
                    if (cost > 500) {       // ob-timeout threshold 500ms
                        AtomicInteger timeoutCount = jobTimeoutCountMap.putIfAbsent(jobId, new AtomicInteger(1));
                        if (timeoutCount != null) {
                            timeoutCount.incrementAndGet();
                        }
                    }

                }

            }
        });
    }



    // ---------------------- helper ----------------------

    private static JobTriggerPoolHelper helper = new JobTriggerPoolHelper();

    public static void toStart() {
        helper.start();
    }
    public static void toStop() {
        helper.stop();
    }

    /**
     * @param jobId
     * @param triggerType
     * @param failRetryCount
     * 			>=0: use this param
     * 			<0: use param from job info config
     * @param executorShardingParam
     * @param executorParam
     *          null: use job param
     *          not null: cover job param
     */
    public static void trigger(int jobId, TriggerTypeEnum triggerType, int failRetryCount, String executorShardingParam, String executorParam, String addressList) {
        helper.addTrigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
    }

}
