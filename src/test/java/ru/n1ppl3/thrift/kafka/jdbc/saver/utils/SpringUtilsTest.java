package ru.n1ppl3.thrift.kafka.jdbc.saver.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ContextConfiguration
@ExtendWith({SpringExtension.class})
class SpringUtilsTest {

    @Autowired
    private ConfigurableEnvironment env;

    @Test
    void test() {
        var propertySources = SpringUtils.getPropertySources(env);
        propertySources.forEach((propertySourceName, properties) -> {
            System.out.println(" - - - - - - >>> " + propertySourceName);
            properties.forEach((name, value) -> System.out.println(name + "=" + value));
        });
    }

}
