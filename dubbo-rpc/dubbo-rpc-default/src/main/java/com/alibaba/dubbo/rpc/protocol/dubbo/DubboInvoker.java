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
package com.alibaba.dubbo.rpc.protocol.dubbo;

import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.AtomicPositiveInteger;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.TimeoutException;
import com.alibaba.dubbo.remoting.exchange.ExchangeClient;
import com.alibaba.dubbo.remoting.exchange.ResponseFuture;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.RpcInvocation;
import com.alibaba.dubbo.rpc.RpcResult;
import com.alibaba.dubbo.rpc.protocol.AbstractInvoker;
import com.alibaba.dubbo.rpc.support.RpcUtils;

/**
 * DubboInvoker(客户端发起远程调用的invoker)
 *
 * @author william.liangf
 * @author chao.liuc
 */
public class DubboInvoker<T> extends AbstractInvoker<T> {

    private final ExchangeClient[]      clients;

    private final AtomicPositiveInteger index = new AtomicPositiveInteger();

    private final String                version;
    
    private final ReentrantLock     destroyLock = new ReentrantLock();
    
    private final Set<Invoker<?>> invokers;
    
    public DubboInvoker(Class<T> serviceType, URL url, ExchangeClient[] clients){
        this(serviceType, url, clients, null);
    }
    
    public DubboInvoker(Class<T> serviceType, URL url, ExchangeClient[] clients, Set<Invoker<?>> invokers){
        super(serviceType, url, new String[] {Constants.INTERFACE_KEY, Constants.GROUP_KEY, Constants.TOKEN_KEY, Constants.TIMEOUT_KEY});
        this.clients = clients;
        // get version.
        this.version = url.getParameter(Constants.VERSION_KEY, "0.0.0");
        this.invokers = invokers; 
    }

    @Override
    /**
     * 发起远程调用（异步或者同步）
     */
    protected Result doInvoke(final Invocation invocation) throws Throwable {
        RpcInvocation inv = (RpcInvocation) invocation;
        final String methodName = RpcUtils.getMethodName(invocation);
        inv.setAttachment(Constants.PATH_KEY, getUrl().getPath());
        inv.setAttachment(Constants.VERSION_KEY, version);
        
        ExchangeClient currentClient;
        if (clients.length == 1) {
            currentClient = clients[0];
        } else {
            currentClient = clients[index.getAndIncrement() % clients.length];
        }
        try {
            boolean isAsync = RpcUtils.isAsync(getUrl(), invocation);
            boolean isOneway = RpcUtils.isOneway(getUrl(), invocation);
            int timeout = getUrl().getMethodParameter(methodName, Constants.TIMEOUT_KEY,Constants.DEFAULT_TIMEOUT);
            if (isOneway) {
            	boolean isSent = getUrl().getMethodParameter(methodName, Constants.SENT_KEY, false);
                currentClient.send(inv, isSent);
                RpcContext.getContext().setFuture(null);
                return new RpcResult();
            } else if (isAsync) {//异步调用 see futureFilter
            	ResponseFuture future = currentClient.request(inv, timeout) ;
                RpcContext.getContext().setFuture(new FutureAdapter<Object>(future));
                return new RpcResult();
            } else {//同步调用
            	RpcContext.getContext().setFuture(null);
                /**
                 * 客户端装饰client发送远程请求(超时等待)
                 * 1.调用过程：
                 *      [client]    ReferenceCountExchangeClient(统计次数的client)->LazyConnectExchangeClient(开启连接的client)->HeaderExchangeClient(默认消息发送的client,开启客户端心跳定时器)->
                 *          |
                 *          v
                 *      [channel]   HeaderExchangeChannel(请求对象封装channel，返回异步请求结果DefaultFuture)->NettyChannel(jboss的channel)->
                 *          |
                 *          v
                 *      [handler]   NettyHandler(存储channel信息的handler)->MultiMessageHandler(跳过)->HeartbeatHandler(心跳消息处理handler)->AllChannelHandler(线程池任务分发handler,跳过)->DecodeHandler(跳过)->HeaderExchangeHandler(记录future的sent时间handler)
                 *
                 * 2.超时等待机制：
                 *   client发起远程每调用request,会原子的创建一个唯一id(mId),作为关联一次调用的request和response；
                 *   (1)client向server发起远程调用，先返回defaultFuture，在调用get()会阻塞当前线程，线程进入超时等待状态，等待server返回的response结果。
                 *   (2)若在超时时间内server响应返回response,通过调用DefaultFuture.received(),根据response中mid找出对应的future，唤醒signal它，返回调用结果；
                 *   (3)若远程调用超时了，此事有个DubboResponseTimeoutScanTimer超时扫描的定时线程，找到超时的future，创建timeoutResponse唤醒通知future，返回超时异常结果
                 */
                return (Result) currentClient.request(inv, timeout).get();
            }
        } catch (TimeoutException e) {
            throw new RpcException(RpcException.TIMEOUT_EXCEPTION, "Invoke remote method timeout. method: " + invocation.getMethodName() + ", provider: " + getUrl() + ", cause: " + e.getMessage(), e);
        } catch (RemotingException e) {
            throw new RpcException(RpcException.NETWORK_EXCEPTION, "Failed to invoke remote method: " + invocation.getMethodName() + ", provider: " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }
    
    @Override
    public boolean isAvailable() {
        if (!super.isAvailable())
            return false;
        for (ExchangeClient client : clients){
            if (client.isConnected() && !client.hasAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY)){
                //cannot write == not Available ?
                return true ;
            }
        }
        return false;
    }

    public void destroy() {
        //防止client被关闭多次.在connect per jvm的情况下，client.close方法会调用计数器-1，当计数器小于等于0的情况下，才真正关闭
        if (super.isDestroyed()){
            return ;
        } else {
            //dubbo check ,避免多次关闭
            destroyLock.lock();
            try{
                if (super.isDestroyed()){
                    return ;
                }
                super.destroy();
                if (invokers != null){
                    invokers.remove(this);
                }
                for (ExchangeClient client : clients) {
                    try {
                        client.close();
                    } catch (Throwable t) {
                        logger.warn(t.getMessage(), t);
                    }
                }
                
            }finally {
                destroyLock.unlock();
            }
        }
    }
}