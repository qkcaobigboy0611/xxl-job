package com.xxl.job.admin.core.trigger;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.model.XxlJobLog;
import com.xxl.job.admin.core.route.ExecutorRouteStrategyEnum;
import com.xxl.job.admin.core.scheduler.XxlJobScheduler;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import com.xxl.job.core.util.IpUtil;
import com.xxl.job.core.util.ThrowableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * xxl-job trigger
 * Created by xuxueli on 17/7/13.
 */
public class XxlJobTrigger {
    private static Logger logger = LoggerFactory.getLogger(XxlJobTrigger.class);

    /**
     * todo 任务触发(入口是从线程池进入)
     */
    public static void trigger(int jobId,
                               TriggerTypeEnum triggerType,
                               int failRetryCount,
                               String executorShardingParam,
                               String executorParam,
                               String addressList) {

        // 加载数据，根据任务ID 获取数据，获得任务对象
        XxlJobInfo jobInfo = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadById(jobId);
        if (jobInfo == null) {
            logger.warn(">>>>>>>>>>>> trigger fail, jobId invalid，jobId={}", jobId);
            return;
        }
        // 任务添加执行器参数
        if (executorParam != null) {
            jobInfo.setExecutorParam(executorParam);
        }
        // 设置失败重试次数
        int finalFailRetryCount = failRetryCount>=0?failRetryCount:jobInfo.getExecutorFailRetryCount();
        // 加载执行器
        XxlJobGroup group = XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().load(jobInfo.getJobGroup());

        // todo 录入执行器地址 ： addressList：服务器地址
        if (addressList!=null && addressList.trim().length()>0) {
            // 注册方式 执行器地址类型：0=自动注册、1=手动录入
            group.setAddressType(1);
            group.setAddressList(addressList.trim());
        }

        // 设置分片参数，分片广播的逻辑 从executorShardingParam中去，应该是我们自己设置的,
        int[] shardingParam = null;
        if (executorShardingParam!=null){
            String[] shardingArr = executorShardingParam.split("/");
            if (shardingArr.length==2 && isNumeric(shardingArr[0]) && isNumeric(shardingArr[1])) {
                shardingParam = new int[2];
                shardingParam[0] = Integer.valueOf(shardingArr[0]);
                shardingParam[1] = Integer.valueOf(shardingArr[1]);
            }
        }
        // 如果是分片广播需要特殊处理
        if (ExecutorRouteStrategyEnum.SHARDING_BROADCAST==ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null)
                && group.getRegistryList()!=null && !group.getRegistryList().isEmpty()
                && shardingParam==null) {
            // 分片广播会通知每一个执行器
            for (int i = 0; i < group.getRegistryList().size(); i++) {
                // todo 正式调用 发起请求调度executor
                processTrigger(group, jobInfo, finalFailRetryCount, triggerType, i, group.getRegistryList().size());
            }
        // 如果不是分片广播
        } else {
            if (shardingParam == null) {
                shardingParam = new int[]{0, 1};
            }
            // todo 正式调用
            processTrigger(group, jobInfo, finalFailRetryCount, triggerType, shardingParam[0], shardingParam[1]);
        }

    }

    private static boolean isNumeric(String str){
        try {
            int result = Integer.valueOf(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * 正式调用
     * @param group ：项目，注册列表，可能为空
     * @param jobInfo：任务信息
     * @param finalFailRetryCount：失败重试次数
     * @param triggerType：调度类型
     * @param index                     sharding index
     * @param total                     sharding index
     */
    private static void processTrigger(XxlJobGroup group, XxlJobInfo jobInfo, int finalFailRetryCount, TriggerTypeEnum triggerType, int index, int total){

        // 阻塞处理策略 并行还是串行：默认是串行，最终要看当前任务设置的是什么
        ExecutorBlockStrategyEnum blockStrategy = ExecutorBlockStrategyEnum.match(jobInfo.getExecutorBlockStrategy(), ExecutorBlockStrategyEnum.SERIAL_EXECUTION);  // block strategy
        // 路由执行策略  默认没有
        ExecutorRouteStrategyEnum executorRouteStrategyEnum = ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null);    // route strategy
        // 分片广播参数设置 ：index,total
        String shardingParam = (ExecutorRouteStrategyEnum.SHARDING_BROADCAST==executorRouteStrategyEnum)?String.valueOf(index).concat("/").concat(String.valueOf(total)):null;

        // 1.todo存储任务执行日志
        XxlJobLog jobLog = new XxlJobLog();
        // 项目ID
        jobLog.setJobGroup(jobInfo.getJobGroup());
        // 任务ID
        jobLog.setJobId(jobInfo.getId());
        // 触发时间
        jobLog.setTriggerTime(new Date());
        // 存储任务执行日志
        XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().save(jobLog);
        logger.debug(">>>>>>>>>>> xxl-job trigger start, jobId:{}", jobLog.getId());

        // 2、初始化请求参数
        TriggerParam triggerParam = new TriggerParam();
        triggerParam.setJobId(jobInfo.getId());// 任务ID
        triggerParam.setExecutorHandler(jobInfo.getExecutorHandler()); //方法名称
        triggerParam.setExecutorParams(jobInfo.getExecutorParam());//任务参数
        triggerParam.setExecutorBlockStrategy(jobInfo.getExecutorBlockStrategy());// 阻塞处理策略
        triggerParam.setExecutorTimeout(jobInfo.getExecutorTimeout());// 任务执行超时时间，单位秒
        triggerParam.setLogId(jobLog.getId());// 日志id
        triggerParam.setLogDateTime(jobLog.getTriggerTime().getTime());// 任务触发时间
        triggerParam.setGlueType(jobInfo.getGlueType());// 运行模式  bean
        triggerParam.setGlueSource(jobInfo.getGlueSource());// GLUE源代码
        triggerParam.setGlueUpdatetime(jobInfo.getGlueUpdatetime().getTime());// GLUE更新时间
        // 分片广播
        triggerParam.setBroadcastIndex(index);
        triggerParam.setBroadcastTotal(total);

        // 3、决策路由执行地址
        // 最后选择的服务器地址
        String address = null;
        // 获取 具体的  地址
        ReturnT<String> routeAddressResult = null;
        // todo 获取手动注入的  项目的 list地址
        if (group.getRegistryList()!=null && !group.getRegistryList().isEmpty()) {
            if (ExecutorRouteStrategyEnum.SHARDING_BROADCAST == executorRouteStrategyEnum) {
                // 分片广播逻辑
                if (index < group.getRegistryList().size()) {
                    address = group.getRegistryList().get(index);
                } else {
                    address = group.getRegistryList().get(0);
                }
            } else {
                // todo 通过我们指定的策略选择地址
                routeAddressResult = executorRouteStrategyEnum.getRouter().route(triggerParam, group.getRegistryList());
                if (routeAddressResult.getCode() == ReturnT.SUCCESS_CODE) {
                    // 获取具体的地址
                    address = routeAddressResult.getContent();
                }
            }
        } else {
            routeAddressResult = new ReturnT<String>(ReturnT.FAIL_CODE, I18nUtil.getString("jobconf_trigger_address_empty"));
        }

        // 4、todo 触发远程执行
        ReturnT<String> triggerResult = null;
        if (address != null) {
            // todo 远程调用executor的run接口,这是真正的调用执行方法
            triggerResult = runExecutor(triggerParam, address);
        } else {
            triggerResult = new ReturnT<String>(ReturnT.FAIL_CODE, null);
        }

        // 5、日志信息拼接
        StringBuffer triggerMsgSb = new StringBuffer();
        triggerMsgSb.append(I18nUtil.getString("jobconf_trigger_type")).append("：").append(triggerType.getTitle());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_admin_adress")).append("：").append(IpUtil.getIp());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_exe_regtype")).append("：")
                .append( (group.getAddressType() == 0)?I18nUtil.getString("jobgroup_field_addressType_0"):I18nUtil.getString("jobgroup_field_addressType_1") );
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_exe_regaddress")).append("：").append(group.getRegistryList());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorRouteStrategy")).append("：").append(executorRouteStrategyEnum.getTitle());
        if (shardingParam != null) {
            triggerMsgSb.append("("+shardingParam+")");
        }
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorBlockStrategy")).append("：").append(blockStrategy.getTitle());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_timeout")).append("：").append(jobInfo.getExecutorTimeout());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorFailRetryCount")).append("：").append(finalFailRetryCount);

        triggerMsgSb.append("<br><br><span style=\"color:#00c0ef;\" > >>>>>>>>>>>"+ I18nUtil.getString("jobconf_trigger_run") +"<<<<<<<<<<< </span><br>")
                .append((routeAddressResult!=null&&routeAddressResult.getMsg()!=null)?routeAddressResult.getMsg()+"<br><br>":"").append(triggerResult.getMsg()!=null?triggerResult.getMsg():"");

        // 6、todo 存储任务执行日志信息
        jobLog.setExecutorAddress(address);// 执行器地址，本次执行的地址
        jobLog.setExecutorHandler(jobInfo.getExecutorHandler());// JobHandler 就是方法名称
        jobLog.setExecutorParam(jobInfo.getExecutorParam());// 执行参数
        jobLog.setExecutorShardingParam(shardingParam);//
        jobLog.setExecutorFailRetryCount(finalFailRetryCount);
        //jobLog.setTriggerTime();
        jobLog.setTriggerCode(triggerResult.getCode());// 执行完成的结果状态
        jobLog.setTriggerMsg(triggerMsgSb.toString());// 触发的结果信息
        // 更新日志信息，todo 都是从配置中获取对象--》执行相应的方法
        XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateTriggerInfo(jobLog);

        logger.debug(">>>>>>>>>>> xxl-job trigger end, jobId:{}", jobLog.getId());
    }

    /**
     * todo 远程 调用项目 ，进行执行对应的方法
     * @param triggerParam todo 触发执行的参数，也就是任务中的各种信息
     * @param address todo 调用的远程地址 调用我们的项目
     * @return
     */
    public static ReturnT<String> runExecutor(TriggerParam triggerParam, String address){
        ReturnT<String> runResult = null;
        try {
            // todo 根据地址获取远程地址服务对象，如果获取不到，就创建一个远程对象，放到map里面
            ExecutorBiz executorBiz = XxlJobScheduler.getExecutorBiz(address);
            runResult = executorBiz.run(triggerParam);
        } catch (Exception e) {
            logger.error(">>>>>>>>>>> xxl-job trigger error, please check if the executor[{}] is running.", address, e);
            runResult = new ReturnT<String>(ReturnT.FAIL_CODE, ThrowableUtil.toString(e));
        }

        StringBuffer runResultSB = new StringBuffer(I18nUtil.getString("jobconf_trigger_run") + "：");
        runResultSB.append("<br>address：").append(address);
        runResultSB.append("<br>code：").append(runResult.getCode());
        runResultSB.append("<br>msg：").append(runResult.getMsg());

        runResult.setMsg(runResultSB.toString());
        return runResult;
    }

}
