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
package org.apache.dubbo.rpc.cluster.support.migration;

import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.util.Optional;

public class MigrationRule {
    private static final String DUBBO_SERVICEDISCOVERY_MIGRATION_KEY = "dubbo.application.service-discovery.migration";
    public static final String DUBBO_SERVICEDISCOVERY_MIGRATION_GROUP = "MIGRATION";
    public static final String RULE_KEY = ApplicationModel.getName() + ".migration";

    private static DynamicConfiguration configuration = null;

    static {
        Optional<DynamicConfiguration> optional = ApplicationModel.getEnvironment().getDynamicConfiguration();
        if (optional.isPresent()) {
            configuration = optional.get();
        }
    }

    private String key;
    private MigrationStep step = MigrationStep.FORCE_INTERFACE;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public MigrationStep getStep() {
        return step;
    }

    public void setStep(MigrationStep step) {
        this.step = step;
    }

    public static MigrationRule parse(String rawRule) {// rawRule --> INIT
        if (null == configuration) {
            return getMigrationRule(null);
        }

        if (StringUtils.isBlank(rawRule) || "INIT".equals(rawRule)) {
            String step = (String)configuration.getInternalProperty(DUBBO_SERVICEDISCOVERY_MIGRATION_KEY); // step --> null
            return getMigrationRule(step);

        }

        Constructor constructor = new Constructor(MigrationRule.class);
        Yaml yaml = new Yaml(constructor);
        return yaml.load(rawRule);
    }

    public static MigrationRule queryRule() {
        if (null == configuration) {
            return getMigrationRule(null);
        }

        String rawRule = configuration.getConfig(MigrationRule.RULE_KEY, DUBBO_SERVICEDISCOVERY_MIGRATION_GROUP);
        return parse(rawRule);
    }

    private  static MigrationRule getMigrationRule(String step) {
        MigrationRule rule = new MigrationRule();
        rule.setStep(Enum.valueOf(MigrationStep.class, StringUtils.isBlank(step) ? MigrationStep.APPLICATION_FIRST.name() : step));
        return rule;
    }
}