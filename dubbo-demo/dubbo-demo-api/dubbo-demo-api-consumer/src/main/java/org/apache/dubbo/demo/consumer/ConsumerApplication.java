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
package org.apache.dubbo.demo.consumer;

import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.apache.dubbo.config.utils.ReferenceConfigCache;
import org.apache.dubbo.demo.DemoService;
import org.apache.dubbo.rpc.service.GenericService;

public class ConsumerApplication {
    public static void main(String[] args) {
        if (isClassic(args)) {
            runWithRefer();
        } else {
            runWithBootstrap();
        }
    }

    /**
     * fixme jiangkui consumer
     *
     * API DEMO：https://dubbo.apache.org/zh/docs/v2.7/user/configuration/api/
     * 架构设计：https://dubbo.apache.org/zh/docs/v2.7/dev/design/
     */
    public void simpleDemo() {
        // 当前应用配置
        ApplicationConfig application = new ApplicationConfig();
        application.setName("yyy");

        // 连接注册中心配置
        RegistryConfig registry = new RegistryConfig();
        registry.setAddress("10.20.130.230:9090");
        registry.setUsername("aaa");
        registry.setPassword("bbb");

        // 注意：ReferenceConfig为重对象，内部封装了与注册中心的连接，以及与服务提供方的连接

        // 引用远程服务
        ReferenceConfig<DemoService> reference = new ReferenceConfig<>(); // 此实例很重，封装了与注册中心的连接以及与提供者的连接，请自行缓存，否则可能造成内存和连接泄漏
        reference.setApplication(application);
        reference.setRegistry(registry); // 多个注册中心可以用setRegistries()
        reference.setInterface(DemoService.class);
        reference.setVersion("1.0.0");

        // 和本地bean一样使用xxxService
        DemoService demoService = reference.get(); // 注意：此代理对象内部封装了所有通讯细节，对象较重，请缓存复用
    }

    private static boolean isClassic(String[] args) {
        return args.length > 0 && "classic".equalsIgnoreCase(args[0]);
    }

    private static void runWithBootstrap() {

        // Spring 应用实例化是由 ReferenceBean 搞的，很简单
        ReferenceConfig<DemoService> reference = new ReferenceConfig<>();
        reference.setInterface(DemoService.class);
        reference.setGeneric("true");

        DubboBootstrap bootstrap = DubboBootstrap.getInstance();
        bootstrap.application(new ApplicationConfig("dubbo-demo-api-consumer"))
                .registry(new RegistryConfig("zookeeper://127.0.0.1:2181"))
                .reference(reference)
                .start();

        DemoService demoService = ReferenceConfigCache.getCache().get(reference);
        String message = demoService.sayHello("dubbo");
        System.out.println("============================" + message);
        System.out.println("============================" + message);
        System.out.println("============================" + message);
        System.out.println("============================" + message);
        System.out.println("============================" + message);

        // generic invoke
        GenericService genericService = (GenericService) demoService;
        Object genericInvokeResult = genericService.$invoke("sayHello", new String[] { String.class.getName() },
                new Object[] { "dubbo generic invoke" });
        System.out.println("++++++++++++++++++++++++++++" + genericInvokeResult);
        System.out.println("++++++++++++++++++++++++++++" + genericInvokeResult);
        System.out.println("++++++++++++++++++++++++++++" + genericInvokeResult);
        System.out.println("++++++++++++++++++++++++++++" + genericInvokeResult);
        System.out.println("++++++++++++++++++++++++++++" + genericInvokeResult);

        // 关掉 Consumer
        bootstrap.unRegisterShutdownHook();
    }

    private static void runWithRefer() {
        ReferenceConfig<DemoService> reference = new ReferenceConfig<>();
        reference.setApplication(new ApplicationConfig("dubbo-demo-api-consumer"));
        reference.setRegistry(new RegistryConfig("zookeeper://127.0.0.1:2181"));
        reference.setInterface(DemoService.class);
        DemoService service = reference.get();
        String message = service.sayHello("dubbo");
        System.out.println(message);
    }
}
