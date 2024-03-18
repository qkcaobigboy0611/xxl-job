package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobRegistry;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.enums.RegistryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.*;

/**
 * job registry instance
 * @author xuxueli 2016-10-02 19:10:24
 */
public class JobRegistryHelper {
	private static Logger logger = LoggerFactory.getLogger(JobRegistryHelper.class);

	private static JobRegistryHelper instance = new JobRegistryHelper();
	public static JobRegistryHelper getInstance(){
		return instance;
	}

	// 定义 注册或者移除 线程池
	private ThreadPoolExecutor registryOrRemoveThreadPool = null;
	// 注册 监控线程
	private Thread registryMonitorThread;
	private volatile boolean toStop = false;

	/**
	 * 创建注册线程池
	 * 创建并启动注册 监控线程
	 */
	public void start(){

		// 创建线程池
		registryOrRemoveThreadPool = new ThreadPoolExecutor(
				2,
				10,
				30L,
				TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>(2000),
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						return new Thread(r, "xxl-job, admin JobRegistryMonitorHelper-registryOrRemoveThreadPool-" + r.hashCode());
					}
				},
				new RejectedExecutionHandler() {
					@Override
					public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
						r.run();
						logger.warn(">>>>>>>>>>> xxl-job, registry or remove too fast, match threadpool rejected handler(run now).");
					}
				});

		// 创建线程 当前线程的目的就是：更新xxl_job_group表中自动注册的执行器
		// 并且删除xxl_job_registry中已经过时的数据
		registryMonitorThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (!toStop) {
					try {
						// 查询数据库全部的 注册类型为自动注册的执行器xxl_job_group
						List<XxlJobGroup> groupList = XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().findByAddressType(0);
						if (groupList!=null && !groupList.isEmpty()) {

							// 查询出过时的地址，超时时间就是项目配置的，也就是查询  xxl_job_registry  ，将过时的都查询出来(sql操作)
							List<Integer> ids = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().findDead(RegistryConfig.DEAD_TIMEOUT, new Date());
							if (ids!=null && ids.size()>0) {
								// 删除超时的数据 xxl_job_registry
								XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().removeDead(ids);
							}

							// 刷新自动注册的最新地址
							HashMap<String, List<String>> appAddressMap = new HashMap<String, List<String>>();
							// 查询出没有超时的 数据 xxl_job_registry
							List<XxlJobRegistry> list = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().findAll(RegistryConfig.DEAD_TIMEOUT, new Date());
							// 往appAddressMap 集合里面存放数据
							// 从查询出没有超时的 全部数据 xxl_job_registry，整理之后放到appAddressMap 集合
							if (list != null) {
								for (XxlJobRegistry item: list) {
									// 如果是自动注册
									if (RegistryConfig.RegistType.EXECUTOR.name().equals(item.getRegistryGroup())) {
										// 根据APP名称获取最新自动注册地址
										String appname = item.getRegistryKey();
										List<String> registryList = appAddressMap.get(appname);
										if (registryList == null) {
											registryList = new ArrayList<String>();
										}

										if (!registryList.contains(item.getRegistryValue())) {
											registryList.add(item.getRegistryValue());
										}
										appAddressMap.put(appname, registryList);
									}
								}
							}

							// 这个里面查询出来的全部的 自动注册的数据，要更新他们的地址
							// groupList 查询数据库全部的 注册类型为 自动注册的 执行器（项目）xxl_job_group
							for (XxlJobGroup group: groupList) {
								// 根据APP名称获取最新自动注册地址
								List<String> registryList = appAddressMap.get(group.getAppname());
								String addressListStr = null;
								if (registryList!=null && !registryList.isEmpty()) {
									Collections.sort(registryList);
									StringBuilder addressListSB = new StringBuilder();
									for (String item:registryList) {
										addressListSB.append(item).append(",");
									}
									addressListStr = addressListSB.toString();
									addressListStr = addressListStr.substring(0, addressListStr.length()-1);
								}
								group.setAddressList(addressListStr);
								group.setUpdateTime(new Date());

								XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().update(group);
							}
						}
					} catch (Exception e) {
						if (!toStop) {
							logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:{}", e);
						}
					}
					try {
						TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
					} catch (InterruptedException e) {
						if (!toStop) {
							logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:{}", e);
						}
					}
				}
				logger.info(">>>>>>>>>>> xxl-job, job registry monitor thread stop");
			}
		});
		// 后台运行
		registryMonitorThread.setDaemon(true);
		registryMonitorThread.setName("xxl-job, admin JobRegistryMonitorHelper-registryMonitorThread");
		registryMonitorThread.start();
	}

	public void toStop(){
		toStop = true;

		// stop registryOrRemoveThreadPool
		registryOrRemoveThreadPool.shutdownNow();

		// stop monitir (interrupt and wait)
		registryMonitorThread.interrupt();
		try {
			registryMonitorThread.join();
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
	}


	// ---------------------- helper ----------------------

	/**
	 * 我们项目 调用  web项目  实现注册功能
	 */
	public ReturnT<String> registry(RegistryParam registryParam) {

		// valid
		if (!StringUtils.hasText(registryParam.getRegistryGroup())
				|| !StringUtils.hasText(registryParam.getRegistryKey())
				|| !StringUtils.hasText(registryParam.getRegistryValue())) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, "Illegal Argument.");
		}

		// 异步执行注册功能
		registryOrRemoveThreadPool.execute(new Runnable() {
			@Override
			public void run() {
				int ret = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().registryUpdate(registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue(), new Date());
				if (ret < 1) {
					XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().registrySave(registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue(), new Date());

					// fresh
					freshGroupRegistryInfo(registryParam);
				}
			}
		});

		return ReturnT.SUCCESS;
	}

	public ReturnT<String> registryRemove(RegistryParam registryParam) {

		// valid
		if (!StringUtils.hasText(registryParam.getRegistryGroup())
				|| !StringUtils.hasText(registryParam.getRegistryKey())
				|| !StringUtils.hasText(registryParam.getRegistryValue())) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, "Illegal Argument.");
		}

		// async execute
		registryOrRemoveThreadPool.execute(new Runnable() {
			@Override
			public void run() {
				int ret = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().registryDelete(registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue());
				if (ret > 0) {
					// fresh
					freshGroupRegistryInfo(registryParam);
				}
			}
		});

		return ReturnT.SUCCESS;
	}

	private void freshGroupRegistryInfo(RegistryParam registryParam){
		// Under consideration, prevent affecting core tables
	}


}
