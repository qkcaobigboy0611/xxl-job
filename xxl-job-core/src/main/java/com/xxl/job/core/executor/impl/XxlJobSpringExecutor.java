package com.xxl.job.core.executor.impl;

import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.glue.GlueFactory;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.lang.reflect.Method;
import java.util.Map;


/**
 * xxlJobExecutor提供注册Job，初始化Server等功能
 *
 * 对于Spring项目的 执行器
 *
 * 初始化执行
 */
public class XxlJobSpringExecutor extends XxlJobExecutor implements ApplicationContextAware, SmartInitializingSingleton, DisposableBean {
    private static final Logger logger = LoggerFactory.getLogger(XxlJobSpringExecutor.class);


    /**
     * SmartInitializingSingleton接口的afterSingletonsInstantiated方法类似bean实例化执行后的回调函数，afterSingletonsInstantiated会在spring容器基本启动完成后执行。
     * 此时所有的单例bean都已经初始化完成，可以在该类中做一些回调操作。
     *
     * XxlJobSpringExecutor，XxlJobSimpleExecutor继承自XxlJobExecutor，同时实现了SmartInitializingSingleton接口
     * XxlJobSpringExecutor 在afterSingletonsInstantiated 方法中，完成了提取@xxlJob注解标注的方法，用于后续任务的执行。
       调用父类方法中的start方法，初始化netty服务器。
     */
    @Override
    public void afterSingletonsInstantiated() {

        // 初始化 Spring里面的方法的处理器
        // 项目启动的时候，已经将所有bean对象放到上下文applicationContext中
        initJobHandlerMethodRepository(applicationContext);

        // 刷新运行工厂
        GlueFactory.refreshInstance(1);

        // 初始化注册，也就是初始化netty服务器
        try {
            super.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // destroy
    @Override
    public void destroy() {
        super.destroy();
    }


    /*private void initJobHandlerRepository(ApplicationContext applicationContext) {
        if (applicationContext == null) {
            return;
        }

        // init job handler action
        Map<String, Object> serviceBeanMap = applicationContext.getBeansWithAnnotation(JobHandler.class);

        if (serviceBeanMap != null && serviceBeanMap.size() > 0) {
            for (Object serviceBean : serviceBeanMap.values()) {
                if (serviceBean instanceof IJobHandler) {
                    String name = serviceBean.getClass().getAnnotation(JobHandler.class).value();
                    IJobHandler handler = (IJobHandler) serviceBean;
                    if (loadJobHandler(name) != null) {
                        throw new RuntimeException("xxl-job jobhandler[" + name + "] naming conflicts.");
                    }
                    registJobHandler(name, handler);
                }
            }
        }
    }*/

    /**
     * 初始化处理器上下文
     * 获取 每一个bean里面的方法对象
     */
    private void initJobHandlerMethodRepository(ApplicationContext applicationContext) {
        if (applicationContext == null) {
            return;
        }
        // 获取所有单列bean name  false：表示只获取单例的
        String[] beanDefinitionNames = applicationContext.getBeanNamesForType(Object.class, false, true);
        for (String beanDefinitionName : beanDefinitionNames) {

            // get bean
            Object bean = null;
            Lazy onBean = applicationContext.findAnnotationOnBean(beanDefinitionName, Lazy.class);
            if (onBean!=null){
                logger.debug("xxl-job annotation scan, skip @Lazy Bean:{}", beanDefinitionName);
                continue;
            }else {
                bean = applicationContext.getBean(beanDefinitionName);
            }

            // 从所有的spring管理器里面有 xxkjob注解 的所有方法
            Map<Method, XxlJob> annotatedMethods = null;   // referred to ：org.springframework.context.event.EventListenerMethodProcessor.processBean
            try {
                annotatedMethods = MethodIntrospector.selectMethods(bean.getClass(),
                        new MethodIntrospector.MetadataLookup<XxlJob>() {
                            @Override
                            public XxlJob inspect(Method method) {
                                return AnnotatedElementUtils.findMergedAnnotation(method, XxlJob.class);
                            }
                        });
            } catch (Throwable ex) {
                logger.error("xxl-job method-jobhandler resolve error for bean[" + beanDefinitionName + "].", ex);
            }
            if (annotatedMethods==null || annotatedMethods.isEmpty()) {
                continue;
            }

            // 遍历每个xxljob注解的方法
            for (Map.Entry<Method, XxlJob> methodXxlJobEntry : annotatedMethods.entrySet()) {
                Method executeMethod = methodXxlJobEntry.getKey();
                // 注解对象
                XxlJob xxlJob = methodXxlJobEntry.getValue();
                // xxljob是注解 bean是类对象，executeMethod 方法对象
                registJobHandler(xxlJob, bean, executeMethod);
            }

        }
    }

    // ---------------------- applicationContext ----------------------
    private static ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        XxlJobSpringExecutor.applicationContext = applicationContext;
    }

    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    /*
    BeanDefinitionRegistryPostProcessor
    registry.getBeanDefine()
    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
        this.registry = registry;
    }
    * */

}
