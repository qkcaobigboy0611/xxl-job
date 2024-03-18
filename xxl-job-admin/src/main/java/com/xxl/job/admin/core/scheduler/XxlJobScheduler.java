package com.xxl.job.admin.core.scheduler;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.thread.*;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.client.ExecutorBizClient;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 任务调度器
 */

public class XxlJobScheduler  {
    private static final Logger logger = LoggerFactory.getLogger(XxlJobScheduler.class);


    public void init() throws Exception {
        // 初始化语言配置 application.properties中的xxl.job.i18n 国际化
        initI18n();

        /**
         * 下面会做一些池化的处理
         */

        // 初始化触发器线程池，初始化两个线程池
        JobTriggerPoolHelper.toStart();

        /**
         * 客户端启动的时候发出请求注册到服务端，会调到服务端的EmbedServer
         */
        // 30s内执行一次，维护注册表信息，判断在线超时时间90s
        /**
         * 保证任务执行的时候，拿到的执行器列表都是运行的
         * 每30s查询数据中自动注册的执行器
         * 查询90s内未再次注册的执行器 register表(默认心跳保活时间为30s)
         * 更新group表的 addressList
         */
        JobRegistryHelper.getInstance().start();

        // 任务失败重试处理  每10ms 执行一次
        // 更新日志的  监控状态
        JobFailMonitorHelper.getInstance().start();

        // 执行器执行任务10min内没有给出结果回复，终止该任务
        // 任务结果丢失处理：调度记录停留在“运行中”状态超过10分钟，并且对应执行器心跳注册失败不在线，
        // 则将本地调度主动标记 失败
        // 就是将超过10分钟还在执行的 任务状态 改为 失败
        // todo 客户端执行完会回调 里面的callback() 会在客户端 /callback 调用AdminBiz.callback
        // todo 接收并处理客户端执行完的结果，调度中心提供的”日志回调服务API服务”代码位置如下：xxl-job-admin#com.xxl.job.admin.controller.JobApiController.callback
        // todo “执行器”在接收到任务执行请求后，执行任务，在执行结束之后会将执行结果回调通知“调度中心”；调度结束之后可以前往控制台（控制页面）查看对应的执行日志。
        JobCompleteHelper.getInstance().start();

        // 统计一些失败成功报表(3天内的报告) 运行报表数据显示 归总日志，清除日志（只是清除数据库）
        JobLogReportHelper.getInstance().start();

        // 执行调度器 : 内核中用来安排进程执行的模块，
        // 它主要完成两件事，一是选择已经就绪的进程执行，二是打断某些执行的进程变为就绪状态，
        // 一直扫描，将即将触发的任务放到时间轴，从时间轴获取"当前秒"需要执行的任务，进行执行
        JobScheduleHelper.getInstance().start();

        logger.info(">>>>>>>>> init xxl-job admin success.");
    }

    
    public void destroy() throws Exception {

        // stop-schedule
        JobScheduleHelper.getInstance().toStop();

        // admin log report stop
        JobLogReportHelper.getInstance().toStop();

        // admin lose-monitor stop
        JobCompleteHelper.getInstance().toStop();

        // admin fail-monitor stop
        JobFailMonitorHelper.getInstance().toStop();

        // admin registry stop
        JobRegistryHelper.getInstance().toStop();

        // admin trigger pool stop
        JobTriggerPoolHelper.toStop();

    }

    // ---------------------- I18n ----------------------
    // todo 初始化 阻塞策略 的 中文名称
    private void initI18n(){
        // 遍历全部的 阻塞策略
        for (ExecutorBlockStrategyEnum item:ExecutorBlockStrategyEnum.values()) {
            item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
        }
    }

    // ---------------------- executor-client ----------------------
    /**
     * todo 据地址获取远程地址服务对象，如果获取不到，就创建一个远程对象，放到map里面
     */
    private static ConcurrentMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<String, ExecutorBiz>();
    public static ExecutorBiz getExecutorBiz(String address) throws Exception {
        // valid
        if (address==null || address.trim().length()==0) {
            return null;
        }

        // load-cache
        address = address.trim();
        ExecutorBiz executorBiz = executorBizRepository.get(address);
        if (executorBiz != null) {
            return executorBiz;
        }

        // set-cache
        executorBiz = new ExecutorBizClient(address, XxlJobAdminConfig.getAdminConfig().getAccessToken());

        executorBizRepository.put(address, executorBiz);
        return executorBiz;
    }

}
