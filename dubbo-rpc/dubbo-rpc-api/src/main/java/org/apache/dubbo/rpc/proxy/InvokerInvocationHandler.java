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
package org.apache.dubbo.rpc.proxy;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Constants;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ConsumerModel;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * InvokerHandler
 */
public class InvokerInvocationHandler implements InvocationHandler {
    private static final Logger logger = LoggerFactory.getLogger(InvokerInvocationHandler.class);
    private final Invoker<?> invoker;
    private ConsumerModel consumerModel;
    private URL url;
    private String protocolServiceKey;

    public static Field stackTraceField;

    static {
        try {
            stackTraceField = Throwable.class.getDeclaredField("stackTrace");
            stackTraceField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            // ignore
        }
    }

    public InvokerInvocationHandler(Invoker<?> handler) {
        this.invoker = handler;
        this.url = invoker.getUrl();
        String serviceKey = this.url.getServiceKey();
        this.protocolServiceKey = this.url.getProtocolServiceKey();
        if (serviceKey != null) {
            this.consumerModel = ApplicationModel.getConsumerModel(serviceKey);
        }
    }

    /**
     * 详情参见：https://dubbo.apache.org/zh/docs/v2.7/dev/source/service-invoking-process/
     *
     * proxy 代理对象中，会生成这么一段代码：
     *
     *      public String sayHello(String string) {
     *         // 将参数存储到 Object 数组中
     *         Object[] arrobject = new Object[]{string};
     *
     *         // 调用 InvocationHandler 实现类的 invoke 方法得到调用结果
     *         // 这段代码会调用到这个方法内
     *         Object object = this.handler.invoke(this, methods[0], arrobject);
     *         // 返回调用结果
     *         return (String)object;
     *     }
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // 拦截定义在 Object 类中的方法（未被子类重写），比如 wait/notify
        if (method.getDeclaringClass() == Object.class) {
            return method.invoke(invoker, args);
        }
        String methodName = method.getName();
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length == 0) {
            // 如果 toString、hashCode 和 equals 等方法被子类重写了，这里也直接调用
            if ("toString".equals(methodName)) {
                return invoker.toString();
            } else if ("$destroy".equals(methodName)) {
                invoker.destroy();
                return null;
            } else if ("hashCode".equals(methodName)) {
                return invoker.hashCode();
            }
        } else if (parameterTypes.length == 1 && "equals".equals(methodName)) {
            return invoker.equals(args[0]);
        }
        RpcInvocation rpcInvocation = new RpcInvocation(method, invoker.getInterface().getName(), protocolServiceKey, args);
        String serviceKey = invoker.getUrl().getServiceKey();
        rpcInvocation.setTargetServiceUniqueName(serviceKey);

        // invoker.getUrl() returns consumer url.
        RpcContext.setRpcContext(invoker.getUrl());

        if (consumerModel != null) {
            rpcInvocation.put(Constants.CONSUMER_MODEL, consumerModel);
            rpcInvocation.put(Constants.METHOD_MODEL, consumerModel.getMethodModel(method));
        }

        // 关键点在这里
        // invoker：MockClusterInvoker
        // Result：AsyncRpcResult
        return invoker.invoke(rpcInvocation).recreate();
    }
}
