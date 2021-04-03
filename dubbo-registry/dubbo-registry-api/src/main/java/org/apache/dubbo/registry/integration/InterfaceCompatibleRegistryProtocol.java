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
package org.apache.dubbo.registry.integration;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.client.ServiceDiscoveryRegistryDirectory;
import org.apache.dubbo.registry.client.migration.MigrationInvoker;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;

import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_PROTOCOL;
import static org.apache.dubbo.registry.Constants.DEFAULT_REGISTRY;

/**
 * RegistryProtocol
 */
public class InterfaceCompatibleRegistryProtocol extends RegistryProtocol {

    @Override
    protected URL getRegistryUrl(Invoker<?> originInvoker) {
        // registryUrl -- registry://127.0.0.1:2181/org.apache.dubbo.registry.RegistryService?application=dubbo-demo-api-provider&dubbo=2.0.2&export=dubbo%3A%2F%2F11.0.94.189%3A20880%2Forg.apache.dubbo.demo.DemoService%3Fanyhost%3Dtrue%26application%3Ddubbo-demo-api-provider%26bind.ip%3D11.0.94.189%26bind.port%3D20880%26default%3Dtrue%26deprecated%3Dfalse%26dubbo%3D2.0.2%26dynamic%3Dtrue%26generic%3Dfalse%26interface%3Dorg.apache.dubbo.demo.DemoService%26methods%3DsayHello%2CsayHelloAsync%26pid%3D93536%26release%3D%26side%3Dprovider%26timestamp%3D1617436492361&pid=93536&registry=zookeeper&timestamp=1617436492328
        URL registryUrl = originInvoker.getUrl();
        if (REGISTRY_PROTOCOL.equals(registryUrl.getProtocol())) {
            String protocol = registryUrl.getParameter(REGISTRY_KEY, DEFAULT_REGISTRY);
            // 删掉了这个：&registry=zookeeper，加上了zookeeper
            registryUrl = registryUrl.setProtocol(protocol).removeParameter(REGISTRY_KEY);
        }
        // registryUrl -- zookeeper://127.0.0.1:2181/org.apache.dubbo.registry.RegistryService?application=dubbo-demo-api-provider&dubbo=2.0.2&export=dubbo%3A%2F%2F11.0.94.189%3A20880%2Forg.apache.dubbo.demo.DemoService%3Fanyhost%3Dtrue%26application%3Ddubbo-demo-api-provider%26bind.ip%3D11.0.94.189%26bind.port%3D20880%26default%3Dtrue%26deprecated%3Dfalse%26dubbo%3D2.0.2%26dynamic%3Dtrue%26generic%3Dfalse%26interface%3Dorg.apache.dubbo.demo.DemoService%26methods%3DsayHello%2CsayHelloAsync%26pid%3D93536%26release%3D%26side%3Dprovider%26timestamp%3D1617436492361&pid=93536&timestamp=1617436492328
        return registryUrl;
    }

    @Override
    protected URL getRegistryUrl(URL url) {
        return URLBuilder.from(url)
                .setProtocol(url.getParameter(REGISTRY_KEY, DEFAULT_REGISTRY))
                .removeParameter(REGISTRY_KEY)
                .build();
    }

    @Override
    public <T> ClusterInvoker<T> getInvoker(Cluster cluster, Registry registry, Class<T> type, URL url) {
        DynamicDirectory<T> directory = new RegistryDirectory<>(type, url);
        return doCreateInvoker(directory, cluster, registry, type);
    }

    @Override
    public <T> ClusterInvoker<T> getServiceDiscoveryInvoker(Cluster cluster, Registry registry, Class<T> type, URL url) {
        registry = registryFactory.getRegistry(super.getRegistryUrl(url));
        DynamicDirectory<T> directory = new ServiceDiscoveryRegistryDirectory<>(type, url);
        return doCreateInvoker(directory, cluster, registry, type);
    }

    protected <T> ClusterInvoker<T> getMigrationInvoker(RegistryProtocol registryProtocol, Cluster cluster, Registry registry, Class<T> type, URL url, URL consumerUrl) {
//        ClusterInvoker<T> invoker = getInvoker(cluster, registry, type, url);
        return new MigrationInvoker<T>(registryProtocol, cluster, registry, type, url, consumerUrl);
    }

}
