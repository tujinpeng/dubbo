/*
 * Copyright 1999-2011 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.remoting.exchange.support;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.TimeoutException;
import com.alibaba.dubbo.remoting.exchange.Request;
import com.alibaba.dubbo.remoting.exchange.Response;
import com.alibaba.dubbo.remoting.exchange.ResponseCallback;
import com.alibaba.dubbo.remoting.exchange.ResponseFuture;

/**
 * <pre>
 * DefaultFuture.
 *
 * 客户端发起一次远程请求的预期结果（requestId--->future）:
 * 工作原理：
 * (1) 客户端发起远程调用，立马返回一个defaultFuture，同时调用get()，客户端线程进入超时等待状态，等待服务器端响应.
 * (2) 若服务器端在超时时间内返回响应，则调用DefaultFuture.received(Channel,Response)通过requestId找到对应的defaultFuturet，通知唤醒等待的客户端线程（此时有response），客户端线程返回最终调用结果；
 * (3) 若服务器端超时响应了，会有一个dubbo线程(DubboResponseTimeoutScanTimer)定时扫描超时的future，生成一个超时的timeoutResponse,调用DefaultFuture.received(Channel,timeoutResponse),通知唤醒等待的客户端线程，客户端返回超时异常
 * </pre>
 * @author qian.lei
 * @author chao.liuc
 */
public class DefaultFuture implements ResponseFuture {

    private static final Logger                   logger = LoggerFactory.getLogger(DefaultFuture.class);

    private static final Map<Long, Channel>       CHANNELS   = new ConcurrentHashMap<Long, Channel>();

    /**
     * 存放请求id和预期结果future的映射
     */
    private static final Map<Long, DefaultFuture> FUTURES   = new ConcurrentHashMap<Long, DefaultFuture>();

    // invoke id.
    private final long                            id;

    private final Channel                         channel;
    
    private final Request                         request;

    private final int                             timeout;

    private final Lock                            lock = new ReentrantLock();

    private final Condition                       done = lock.newCondition();

    private final long                            start = System.currentTimeMillis();

    private volatile long                         sent;
    
    private volatile Response                     response;

    private volatile ResponseCallback             callback;

    public DefaultFuture(Channel channel, Request request, int timeout){
        this.channel = channel;
        this.request = request;
        this.id = request.getId();
        this.timeout = timeout > 0 ? timeout : channel.getUrl().getPositiveParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT);
        // put into waiting map.
        FUTURES.put(id, this);
        CHANNELS.put(id, channel);
    }

    /**
     * 当客户端发起请求时，调用对应future的get()方法进行超时等待响应
     * @return
     * @throws RemotingException
     */
    public Object get() throws RemotingException {
        return get(timeout);
    }

    public Object get(int timeout) throws RemotingException {
        if (timeout <= 0) {
            //设置默认超时时间
            timeout = Constants.DEFAULT_TIMEOUT;
        }
        //1.若此时还没有返回response，消费者线程则进入超时等待过程
        if (! isDone()) {
            long start = System.currentTimeMillis();
            lock.lock();//对future的response访问又竞争 因此要加可重入锁
            try {
                while (! isDone()) {
                    done.await(timeout, TimeUnit.MILLISECONDS);//超时等待 当服务器端返回response时，调用DefaultFuture.received()唤醒这里
                    if (isDone() || System.currentTimeMillis() - start > timeout) {//有返回或者超时了，结束等待
                        break;
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
            //客户端线程结束超时等待，若还没有响应(可能超时检查线程没有扫描到)，此时可判定为超时，客户端线程直接返回超时异常
            if (! isDone()) {
                throw new TimeoutException(sent > 0, channel, getTimeoutMessage(false));
            }
        }
        // 2.消费者线程结束等待 ，判断若是超时返回的直接抛异常；正常返回的，直接返回调用结果
        return returnFromResponse();
    }
    
    public void cancel(){
        Response errorResult = new Response(id);
        errorResult.setErrorMessage("request future has been canceled.");
        response = errorResult ;
        FUTURES.remove(id);
        CHANNELS.remove(id);
    }

    public boolean isDone() {
        return response != null;
    }

    public void setCallback(ResponseCallback callback) {
        if (isDone()) {
            invokeCallback(callback);
        } else {
            boolean isdone = false;
            lock.lock();
            try{
                if (!isDone()) {
                    this.callback = callback;
                } else {
                    isdone = true;
                }
            }finally {
                lock.unlock();
            }
            if (isdone){
                invokeCallback(callback);
            }
        }
    }
    private void invokeCallback(ResponseCallback c){
        ResponseCallback callbackCopy = c;
        if (callbackCopy == null){
            throw new NullPointerException("callback cannot be null.");
        }
        c = null;
        Response res = response;
        if (res == null) {
            throw new IllegalStateException("response cannot be null. url:"+channel.getUrl());
        }
        
        if (res.getStatus() == Response.OK) {
            try {
                callbackCopy.done(res.getResult());
            } catch (Exception e) {
                logger.error("callback invoke error .reasult:" + res.getResult() + ",url:" + channel.getUrl(), e);
            }
        } else if (res.getStatus() == Response.CLIENT_TIMEOUT || res.getStatus() == Response.SERVER_TIMEOUT) {
            try {
                TimeoutException te = new TimeoutException(res.getStatus() == Response.SERVER_TIMEOUT, channel, res.getErrorMessage());
                callbackCopy.caught(te);
            } catch (Exception e) {
                logger.error("callback invoke error ,url:" + channel.getUrl(), e);
            }
        } else {
            try {
                RuntimeException re = new RuntimeException(res.getErrorMessage());
                callbackCopy.caught(re);
            } catch (Exception e) {
                logger.error("callback invoke error ,url:" + channel.getUrl(), e);
            }
        }
    }

    private Object returnFromResponse() throws RemotingException {
        Response res = response;
        if (res == null) {
            throw new IllegalStateException("response cannot be null");
        }
        if (res.getStatus() == Response.OK) {
            return res.getResult();
        }
        if (res.getStatus() == Response.CLIENT_TIMEOUT || res.getStatus() == Response.SERVER_TIMEOUT) {
            throw new TimeoutException(res.getStatus() == Response.SERVER_TIMEOUT, channel, res.getErrorMessage());
        }
        throw new RemotingException(channel, res.getErrorMessage());
    }

    private long getId() {
        return id;
    }
    
    private Channel getChannel() {
        return channel;
    }
    
    private boolean isSent() {
        return sent > 0;
    }

    public Request getRequest() {
        return request;
    }

    private int getTimeout() {
        return timeout;
    }

    private long getStartTimestamp() {
        return start;
    }

    public static DefaultFuture getFuture(long id) {
        return FUTURES.get(id);
    }

    public static boolean hasFuture(Channel channel) {
        return CHANNELS.containsValue(channel);
    }

    public static void sent(Channel channel, Request request) {
        DefaultFuture future = FUTURES.get(request.getId());
        if (future != null) {
            future.doSent();
        }
    }

    private void doSent() {
        sent = System.currentTimeMillis();
    }

    /**
     * 客户端接收到服务端返回时或者超时检查线程扫描到超时future会调用，通知等待的future有response
     * @param channel
     * @param response
     */
    public static void received(Channel channel, Response response) {
        try {
            //request和response通过mid关联,可以通过id找到对应的future
            DefaultFuture future = FUTURES.remove(response.getId());
            if (future != null) {
                future.doReceived(response);
            } else {
                logger.warn("The timeout response finally returned at " 
                            + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())) 
                            + ", response " + response 
                            + (channel == null ? "" : ", channel: " + channel.getLocalAddress() 
                                + " -> " + channel.getRemoteAddress()));
            }
        } finally {
            CHANNELS.remove(response.getId());
        }
    }

    private void doReceived(Response res) {
        lock.lock();
        try {
            response = res;
            if (done != null) {
                done.signal();
            }
        } finally {
            lock.unlock();
        }
        if (callback != null) {
            invokeCallback(callback);
        }
    }

    private String getTimeoutMessage(boolean scan) {
        long nowTimestamp = System.currentTimeMillis();
        return (sent > 0 ? "Waiting server-side response timeout" : "Sending request timeout in client-side")
                    + (scan ? " by scan timer" : "") + ". start time: " 
                    + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(start))) + ", end time: " 
                    + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())) + ","
                    + (sent > 0 ? " client elapsed: " + (sent - start) 
                        + " ms, server elapsed: " + (nowTimestamp - sent)
                        : " elapsed: " + (nowTimestamp - start)) + " ms, timeout: "
                    + timeout + " ms, request: " + request + ", channel: " + channel.getLocalAddress()
                    + " -> " + channel.getRemoteAddress();
    }

    /**
     * 检查调用超时的定时线程
     */
    private static class RemotingInvocationTimeoutScan implements Runnable {

        public void run() {
            while (true) {
                try {
                    for (DefaultFuture future : FUTURES.values()) {
                        if (future == null || future.isDone()) {
                            continue;
                        }
                        //检测到furure超时
                        if (System.currentTimeMillis() - future.getStartTimestamp() > future.getTimeout()) {
                            //创建一个具有相同requestId的超时response
                            // create exception response.
                            Response timeoutResponse = new Response(future.getId());
                            // set timeout status.
                            // 设置超时状态(若此时future对应请求还没有发送给服务端，则判定为客户端超时否则服务端超时)
                            timeoutResponse.setStatus(future.isSent() ? Response.SERVER_TIMEOUT : Response.CLIENT_TIMEOUT);
                            timeoutResponse.setErrorMessage(future.getTimeoutMessage(true));
                            // handle response.
                            //通知等待的future
                            DefaultFuture.received(future.getChannel(), timeoutResponse);
                        }
                    }
                    Thread.sleep(30);
                } catch (Throwable e) {
                    logger.error("Exception when scan the timeout invocation of remoting.", e);
                }
            }
        }
    }

    static {
        Thread th = new Thread(new RemotingInvocationTimeoutScan(), "DubboResponseTimeoutScanTimer");
        th.setDaemon(true);
        th.start();
    }

}