/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dubbo.remoting.exchange.support.header;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.transport.AbstractChannelHandlerDelegate;

import static org.apache.dubbo.common.constants.CommonConstants.HEARTBEAT_EVENT;

public class HeartbeatHandler extends AbstractChannelHandlerDelegate {

    private static final Logger logger = LoggerFactory.getLogger(HeartbeatHandler.class);

    public static final String KEY_READ_TIMESTAMP = "READ_TIMESTAMP";

    public static final String KEY_WRITE_TIMESTAMP = "WRITE_TIMESTAMP";

    public HeartbeatHandler(ChannelHandler handler) {
        super(handler);
    }

    @Override
    public void connected(Channel channel) throws RemotingException {
        setReadTimestamp(channel);
        setWriteTimestamp(channel);
        handler.connected(channel);
    }

    @Override
    public void disconnected(Channel channel) throws RemotingException {
        clearReadTimestamp(channel);
        clearWriteTimestamp(channel);
        handler.disconnected(channel);
    }

    @Override
    public void sent(Channel channel, Object message) throws RemotingException {
        setWriteTimestamp(channel);
        handler.sent(channel, message);
    }

    /**
     * 如果是心跳消息，则拦截处理
     */
    @Override
    public void received(Channel channel, Object message) throws RemotingException {
        setReadTimestamp(channel);
        if (isHeartbeatRequest(message)) {
            Request req = (Request) message;
            if (req.isTwoWay()) {
                Response res = new Response(req.getId(), req.getVersion());
                res.setEvent(HEARTBEAT_EVENT);
                channel.send(res);
                if (logger.isInfoEnabled()) {
                    int heartbeat = channel.getUrl().getParameter(Constants.HEARTBEAT_KEY, 0);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Received heartbeat from remote channel " + channel.getRemoteAddress()
                                + ", cause: The channel has no data-transmission exceeds a heartbeat period"
                                + (heartbeat > 0 ? ": " + heartbeat + "ms" : ""));
                    }
                }
            }
            return;
        }
        if (isHeartbeatResponse(message)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Receive heartbeat response in thread " + Thread.currentThread().getName());
            }
            return;
        }
        /*
            `SPI：org.apache.dubbo.remoting.Dispatcher`
                - >all=org.apache.dubbo.remoting.transport.dispatcher.all.AllDispatcher（默认） --> 所有消息都派发到线程池，包括请求，响应，连接事件，断开事件等
                   direct=org.apache.dubbo.remoting.transport.dispatcher.direct.DirectDispatcher --> 所有消息都不派发到线程池，全部在 IO 线程上直接执行
                   message=org.apache.dubbo.remoting.transport.dispatcher.message.MessageOnlyDispatcher --> 只有请求和响应消息派发到线程池，其它消息均在 IO 线程上执行
                   execution=org.apache.dubbo.remoting.transport.dispatcher.execution.ExecutionDispatcher --> 只有请求消息派发到线程池，不含响应。其它消息均在 IO 线程上执行
                   connection=org.apache.dubbo.remoting.transport.dispatcher.connection.ConnectionOrderedDispatcher --> 在 IO 线程上，将连接断开事件放入队列，有序逐个执行，其它消息派发到线程池
                - IO线程：Dubbo 把底层通信框架中接收请求的线程称为 IO 线程
                - 线程派发：如果处理比较耗时，则需要用线程池来处理，原因是：IO 线程主要用于接收请求，如果 IO 线程被占满，将导致它不能接收新的请求。
                - [Dubbo官方文档：2.3.2.1 线程派发模型](https://dubbo.apache.org/zh/docs/v2.7/dev/source/service-invoking-process/#2321-%E7%BA%BF%E7%A8%8B%E6%B4%BE%E5%8F%91%E6%A8%A1%E5%9E%8B)

            这里是：AllDispatcher(创建 AllChannelHandler) --> AllChannelHandler#received()
         */
        handler.received(channel, message);
    }

    private void setReadTimestamp(Channel channel) {
        channel.setAttribute(KEY_READ_TIMESTAMP, System.currentTimeMillis());
    }

    private void setWriteTimestamp(Channel channel) {
        channel.setAttribute(KEY_WRITE_TIMESTAMP, System.currentTimeMillis());
    }

    private void clearReadTimestamp(Channel channel) {
        channel.removeAttribute(KEY_READ_TIMESTAMP);
    }

    private void clearWriteTimestamp(Channel channel) {
        channel.removeAttribute(KEY_WRITE_TIMESTAMP);
    }

    private boolean isHeartbeatRequest(Object message) {
        return message instanceof Request && ((Request) message).isHeartbeat();
    }

    private boolean isHeartbeatResponse(Object message) {
        return message instanceof Response && ((Response) message).isHeartbeat();
    }
}
