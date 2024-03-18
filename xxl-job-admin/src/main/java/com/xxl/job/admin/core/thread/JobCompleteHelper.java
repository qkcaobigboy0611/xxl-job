package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.complete.XxlJobCompleter;
import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobLog;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

/**
 * 任务完成线程
 * 处理任务结果的线程 回调监听器
 * 就是将超过10分钟还在执行的任务状态更改为 失败
 */
public class JobCompleteHelper {
	private static Logger logger = LoggerFactory.getLogger(JobCompleteHelper.class);
	
	private static JobCompleteHelper instance = new JobCompleteHelper();
	public static JobCompleteHelper getInstance(){
		return instance;
	}

	// ---------------------- monitor ----------------------
	// 回调函数线程池
	private ThreadPoolExecutor callbackThreadPool = null;
	// 定义监控线程
	private Thread monitorThread;
	private volatile boolean toStop = false;
	public void start(){

		// 定义回调函数线程池
		callbackThreadPool = new ThreadPoolExecutor(
				2,
				20,
				30L,
				TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>(3000),
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						return new Thread(r, "xxl-job, admin JobLosedMonitorHelper-callbackThreadPool-" + r.hashCode());
					}
				},
				new RejectedExecutionHandler() {
					@Override
					public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
						r.run();
						logger.warn(">>>>>>>>>>> xxl-job, callback too fast, match threadpool rejected handler(run now).");
					}
				});


		// 定义监控线程
		monitorThread = new Thread(new Runnable() {

			@Override
			public void run() {

				// 等触发线程池创建之后，监控线程池才创建
				try {
					// 所以休眠一段时间
					TimeUnit.MILLISECONDS.sleep(50);
				} catch (InterruptedException e) {
					if (!toStop) {
						logger.error(e.getMessage(), e);
					}
				}

				// 监控日志
				while (!toStop) {
					try {
						// 任务结果丢失处理：调度记录停留在 "运行中" 状态超过10min，
						//且对应执行器心跳注册失败不在线，则将本地调度主动标记失败；
						Date losedTime = DateUtil.addMinutes(new Date(), -10);
						// 从xxl-job-log 表中获取数据，是触发成功，但是正在执行的日志
						List<Long> losedJobIds  = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().findLostJobIds(losedTime);

						if (losedJobIds!=null && losedJobIds.size()>0) {
							// 遍历每一个日志
							for (Long logId: losedJobIds) {

								XxlJobLog jobLog = new XxlJobLog();
								jobLog.setId(logId);

								jobLog.setHandleTime(new Date());
								// 更改执行状态为失败
								jobLog.setHandleCode(ReturnT.FAIL_CODE);
								// 执行失败 日志
								jobLog.setHandleMsg( I18nUtil.getString("joblog_lost_fail") );

								XxlJobCompleter.updateHandleInfoAndFinish(jobLog);
							}

						}
					} catch (Exception e) {
						if (!toStop) {
							logger.error(">>>>>>>>>>> xxl-job, job fail monitor thread error:{}", e);
						}
					}

                    try {
                        TimeUnit.SECONDS.sleep(60);
                    } catch (Exception e) {
                        if (!toStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }

                }

				logger.info(">>>>>>>>>>> xxl-job, JobLosedMonitorHelper stop");

			}
		});
		monitorThread.setDaemon(true);
		monitorThread.setName("xxl-job, admin JobLosedMonitorHelper");
		monitorThread.start();
	}

	public void toStop(){
		toStop = true;

		// stop registryOrRemoveThreadPool
		callbackThreadPool.shutdownNow();

		// stop monitorThread (interrupt and wait)
		monitorThread.interrupt();
		try {
			monitorThread.join();
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
	}


	// ---------------------- helper ----------------------

	/**
	 * 我们项目调用 web实现 回调功能
	 */
	public ReturnT<String> callback(List<HandleCallbackParam> callbackParamList) {
		// 我们项目初始化的时候 就创建这个线程池
		callbackThreadPool.execute(new Runnable() {
			@Override
			public void run() {
				for (HandleCallbackParam handleCallbackParam: callbackParamList) {
					// 遍历 每一个回调的  信息
					ReturnT<String> callbackResult = callback(handleCallbackParam);
					logger.debug(">>>>>>>>> JobApiController.callback {}, handleCallbackParam={}, callbackResult={}",
							(callbackResult.getCode()== ReturnT.SUCCESS_CODE?"success":"fail"), handleCallbackParam, callbackResult);
				}
			}
		});

		return ReturnT.SUCCESS;
	}

	private ReturnT<String> callback(HandleCallbackParam handleCallbackParam) {
		// 从xxl_job_log 获取当前日志的信息
		XxlJobLog log = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().load(handleCallbackParam.getLogId());
		if (log == null) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, "log item not found.");
		}
		// 避免重复回调
		if (log.getHandleCode() > 0) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, "log repeate callback.");     // avoid repeat callback, trigger child job etc
		}

		// 执行结果
		StringBuffer handleMsg = new StringBuffer();
		if (log.getHandleMsg()!=null) {
			handleMsg.append(log.getHandleMsg()).append("<br>");
		}
		if (handleCallbackParam.getHandleMsg() != null) {
			handleMsg.append(handleCallbackParam.getHandleMsg());
		}

		// success, save log
		log.setHandleTime(new Date());
		log.setHandleCode(handleCallbackParam.getHandleCode());
		// 执行结果
		log.setHandleMsg(handleMsg.toString());
		// 更新日志
		XxlJobCompleter.updateHandleInfoAndFinish(log);

		return ReturnT.SUCCESS;
	}



}
