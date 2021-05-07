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
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.List;

/**
 * 广播调用：会逐个调用每个服务提供者（每个都通知一遍），如果其中一台报错，在循环调用结束后，BroadcastClusterInvoker 会抛出异常。该类通常用于通知所有提供者更新缓存或日志等本地资源信息。
 *
 * 原文：https://dubbo.apache.org/zh/docs/v2.7/dev/source/cluster/#m-zhdocsv27devsourcecluster
 *
 * BroadcastClusterInvoker
 */
public class BroadcastClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(BroadcastClusterInvoker.class);
    private static final String BROADCAST_FAIL_PERCENT_KEY = "broadcast.fail.percent";
    private static final int MAX_BROADCAST_FAIL_PERCENT = 100;
    private static final int MIN_BROADCAST_FAIL_PERCENT = 0;

    public BroadcastClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Result doInvoke(final Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        checkInvokers(invokers, invocation);
        RpcContext.getContext().setInvokers((List) invokers);
        RpcException exception = null;
        Result result = null;
        URL url = getUrl();
        // The value range of broadcast.fail.threshold must be 0～100.
        // 100 means that an exception will be thrown last, and 0 means that as long as an exception occurs, it will be thrown.
        // see https://github.com/apache/dubbo/pull/7174
        int broadcastFailPercent = url.getParameter(BROADCAST_FAIL_PERCENT_KEY, MAX_BROADCAST_FAIL_PERCENT);

        if (broadcastFailPercent < MIN_BROADCAST_FAIL_PERCENT || broadcastFailPercent > MAX_BROADCAST_FAIL_PERCENT) {
            logger.info(String.format("The value corresponding to the broadcast.fail.percent parameter must be between 0 and 100. " +
                    "The current setting is %s, which is reset to 100.", broadcastFailPercent));
            broadcastFailPercent = MAX_BROADCAST_FAIL_PERCENT;
        }

        int failThresholdIndex = invokers.size() * broadcastFailPercent / MAX_BROADCAST_FAIL_PERCENT;
        int failIndex = 0;

        // 遍历 Invoker 列表，逐个调用
        for (Invoker<T> invoker : invokers) {
            try {
                // 进行远程调用
                result = invoker.invoke(invocation);
                if (null != result && result.hasException()) {
                    Throwable resultException = result.getException();
                    if (null != resultException) {
                        exception = getRpcException(result.getException());
                        logger.warn(exception.getMessage(), exception);
                        if (failIndex == failThresholdIndex) {
                            break;
                        } else {
                            failIndex++;
                        }
                    }
                }
            } catch (Throwable e) {
                exception = getRpcException(e);
                logger.warn(exception.getMessage(), exception);
                if (failIndex == failThresholdIndex) {
                    break;
                } else {
                    failIndex++;
                }
            }
        }

        // 都调用完毕后，如果 exception 不为空，则抛出异常
        if (exception != null) {
            if (failIndex == failThresholdIndex) {
                logger.debug(
                        String.format("The number of BroadcastCluster call failures has reached the threshold %s", failThresholdIndex));
            } else {
                logger.debug(String.format("The number of BroadcastCluster call failures has not reached the threshold %s, fail size is %s",
                        failIndex));
            }
            throw exception;
        }

        return result;
    }

    private RpcException getRpcException(Throwable throwable) {
        RpcException rpcException = null;
        if (throwable instanceof RpcException) {
            rpcException = (RpcException) throwable;
        } else {
            rpcException = new RpcException(throwable.getMessage(), throwable);
        }
        return rpcException;
    }
}
