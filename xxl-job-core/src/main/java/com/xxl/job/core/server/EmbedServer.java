package com.xxl.job.core.server;

import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.impl.ExecutorBizImpl;
import com.xxl.job.core.biz.model.*;
import com.xxl.job.core.thread.ExecutorRegistryThread;
import com.xxl.job.core.util.GsonTool;
import com.xxl.job.core.util.ThrowableUtil;
import com.xxl.job.core.util.XxlJobRemotingUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * Copy from : https://github.com/xuxueli/xxl-rpc
 *  注册 服务
 *  将我们的项目 注册  到  web里面
 *  任务调度的 RPC 服务
 * @author xuxueli 2020-04-11 21:25
 */
public class EmbedServer {
    private static final Logger logger = LoggerFactory.getLogger(EmbedServer.class);

    private ExecutorBiz executorBiz;
    private Thread thread;

    public void start(final String address, final int port, final String appname, final String accessToken) {
        executorBiz = new ExecutorBizImpl();
        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                // NioEventLoopGroup是处理netty连接和IO操作的NioEventLoop执行者组
                // 对于服务端，netty会创建两个不同类型的组，一个成为bossGroup负责处理accept事件，
                // 一个为workerGroup负责处理read/write/connect事件。
                EventLoopGroup bossGroup = new NioEventLoopGroup();
                EventLoopGroup workerGroup = new NioEventLoopGroup();
                ThreadPoolExecutor bizThreadPool = new ThreadPoolExecutor(
                        0,
                        200,
                        60L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<Runnable>(2000),
                        new ThreadFactory() {
                            @Override
                            public Thread newThread(Runnable r) {
                                return new Thread(r, "xxl-job, EmbedServer bizThreadPool-" + r.hashCode());
                            }
                        },
                        new RejectedExecutionHandler() {
                            @Override
                            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                                throw new RuntimeException("xxl-job, EmbedServer bizThreadPool is EXHAUSTED!");
                            }
                        });
                try {
                    // 启动netty服务器
                    // 主要是将我们配置文件写的端口启动一个Netty进程，
                    // 然后监听这个端口号，读取调度中心的调度数据，
                    // 做相应的任务执行
                    ServerBootstrap bootstrap = new ServerBootstrap();
                    bootstrap.group(bossGroup, workerGroup)
                            .channel(NioServerSocketChannel.class)
                            .childHandler(new ChannelInitializer<SocketChannel>() {
                                @Override
                                public void initChannel(SocketChannel channel) throws Exception {
                                    channel.pipeline()
                                            // channelPiple中添加IdleStateHandler，用于触发定时任务，使用它实现心跳机制，
                                            // 如果一段时间没有收到心跳包的话，从而及时重新建立连接，可以方便的实现心跳机制，保存长连接
                                            .addLast(new IdleStateHandler(0, 0, 30 * 3, TimeUnit.SECONDS))  // beat 3N, close if idle
                                            // 二进制数据与Java对象之间进行编码和解码的组件
                                            .addLast(new HttpServerCodec())
                                            // HTTP1.1协议支持长连接，可以在一个TCP连接上多次发送请求和响应，在netty中可以使用HttpClientCodec和HttpObjectAggregator处理器，实现基于HTTP 1.1协议的长连接。
                                            .addLast(new HttpObjectAggregator(5 * 1024 * 1024))  // merge request & reponse to FULL
                                            // todo RPC主要逻辑
                                            .addLast(new EmbedHttpServerHandler(executorBiz, accessToken, bizThreadPool));
                                }
                            })
                            .childOption(ChannelOption.SO_KEEPALIVE, true);

                    // bind 异步绑定
                    ChannelFuture future = bootstrap.bind(port).sync();

                    logger.info(">>>>>>>>>>> xxl-job remoting server start success, nettype = {}, port = {}", EmbedServer.class, port);

                    // todo 开始注册
                    startRegistry(appname, address);

                    // wait util stop
                    future.channel().closeFuture().sync();

                } catch (InterruptedException e) {
                    logger.info(">>>>>>>>>>> xxl-job remoting server stop.");
                } catch (Exception e) {
                    logger.error(">>>>>>>>>>> xxl-job remoting server error.", e);
                } finally {
                    // stop
                    try {
                        workerGroup.shutdownGracefully();
                        bossGroup.shutdownGracefully();
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        });
        thread.setDaemon(true);    // daemon, service jvm, user thread leave >>> daemon leave >>> jvm leave
        thread.start();
    }

    public void stop() throws Exception {
        // destroy server thread
        if (thread != null && thread.isAlive()) {
            thread.interrupt();
        }

        // stop registry
        stopRegistry();
        logger.info(">>>>>>>>>>> xxl-job remoting server destroy success.");
    }


    // ---------------------- registry ----------------------

    /**
     * netty_http
     * <p>
     * Copy from : https://github.com/xuxueli/xxl-rpc
     * 来处理调度中心调度执行器中的任务
     * todo 在客户端和服务端的ChannelPipeline中添加IdleState
     * @author xuxueli 2015-11-24 22:25:15
     */
    public static class EmbedHttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        private static final Logger logger = LoggerFactory.getLogger(EmbedHttpServerHandler.class);

        private ExecutorBiz executorBiz;
        private String accessToken;
        private ThreadPoolExecutor bizThreadPool;

        public EmbedHttpServerHandler(ExecutorBiz executorBiz, String accessToken, ThreadPoolExecutor bizThreadPool) {
            this.executorBiz = executorBiz;
            this.accessToken = accessToken;
            this.bizThreadPool = bizThreadPool;
        }

        /**
         * todo netty监测这些接口，远程调用这些地址执行，获取返回结果
         * 当调度中心发起请求的时候，核心处理逻辑都在这个channelRead0方法里面。
         * @param ctx
         * @param msg
         * @throws Exception
         */
        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
            // todo 收到心跳包
            //final byte[] requestBytes = ByteBufUtil.getBytes(msg.content());    // byteBuf.toString(io.netty.util.CharsetUtil.UTF_8);
            String requestData = msg.content().toString(CharsetUtil.UTF_8);
            String uri = msg.uri();
            HttpMethod httpMethod = msg.method();
            boolean keepAlive = HttpUtil.isKeepAlive(msg);
            String accessTokenReq = msg.headers().get(XxlJobRemotingUtil.XXL_JOB_ACCESS_TOKEN);

            // 线程池执行 项目初始化的时候，就创建了这个
            bizThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    // todo 进行处理 调用远程地址进行执行，获取返回结果
                    Object responseObj = process(httpMethod, uri, requestData, accessTokenReq);

                    // to json
                    String responseJson = GsonTool.toJson(responseObj);

                    // 发送心跳包
                    writeResponse(ctx, keepAlive, responseJson);
                }
            });
        }

        /**
         * todo 执行器
         * @param httpMethod
         * @param uri
         * @param requestData
         * @param accessTokenReq
         * @return
         */
        private Object process(HttpMethod httpMethod, String uri, String requestData, String accessTokenReq) {
            // valid
            if (HttpMethod.POST != httpMethod) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, HttpMethod not support.");
            }
            if (uri == null || uri.trim().length() == 0) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping empty.");
            }
            if (accessToken != null
                    && accessToken.trim().length() > 0
                    && !accessToken.equals(accessTokenReq)) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "The access token is wrong.");
            }

            // services mapping
            try {
                switch (uri) {
                    // todo 调度中心检测执行器是否在线时使用
                    case "/beat": //web项目验证我们自己的项目是不是启动 的，心跳检查，我们的项目可以走到这里
                        // 说明我们的项目 启动的，就返回success ，就告诉web，我们 项目启动的了
                        return executorBiz.beat();
                    case "/idleBeat": //空闲  心跳  检查
                        IdleBeatParam idleBeatParam = GsonTool.fromJson(requestData, IdleBeatParam.class);
                        return executorBiz.idleBeat(idleBeatParam);

                        // todo 触发任务执行
                    case "/run": // web 端传过来 run  运行任务
                        TriggerParam triggerParam = GsonTool.fromJson(requestData, TriggerParam.class);
                        return executorBiz.run(triggerParam);
                    case "/kill":
                        KillParam killParam = GsonTool.fromJson(requestData, KillParam.class);
                        return executorBiz.kill(killParam);

                        // todo 终止任务，滚动方式加载
                    case "/log":
                        LogParam logParam = GsonTool.fromJson(requestData, LogParam.class);
                        return executorBiz.log(logParam);
                    default:
                        return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping(" + uri + ") not found.");
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return new ReturnT<String>(ReturnT.FAIL_CODE, "request error:" + ThrowableUtil.toString(e));
            }
        }

        /**
         * write response
         */
        private void writeResponse(ChannelHandlerContext ctx, boolean keepAlive, String responseJson) {
            // write response
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer(responseJson, CharsetUtil.UTF_8));   //  Unpooled.wrappedBuffer(responseJson)
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html;charset=UTF-8");       // HttpHeaderValues.TEXT_PLAIN.toString()
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            if (keepAlive) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }
            // todo 通过ChannelHandlerContext写入并发送消息，等同于连续调用 write()和flush()方法
            // 另外可以使用Netty提供的ChannelFuture对象来监听操作结果或者进行异步操作
            ctx.writeAndFlush(response);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error(">>>>>>>>>>> xxl-job provider netty_http server caught exception", cause);
            ctx.close();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                ctx.channel().close();      // beat 3N, close if idle
                logger.debug(">>>>>>>>>>> xxl-job provider netty_http server close an idle channel.");
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }

    // ---------------------- registry ----------------------

    public void startRegistry(final String appname, final String address) {
        // start registry
        ExecutorRegistryThread.getInstance().start(appname, address);
    }

    public void stopRegistry() {
        // stop registry
        ExecutorRegistryThread.getInstance().toStop();
    }
}
