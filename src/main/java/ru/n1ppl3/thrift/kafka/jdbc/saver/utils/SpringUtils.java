package ru.n1ppl3.thrift.kafka.jdbc.saver.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public abstract class SpringUtils {


    public static Map<String, Map<String, Object>> getPropertySources(ConfigurableEnvironment env) {
        Map<String, Map<String, Object>> result = new HashMap<>();

        env.getPropertySources().forEach(propertySource -> {
            if (propertySource instanceof EnumerablePropertySource) {
                Map<String, Object> properties = new HashMap<>();
                EnumerablePropertySource<?> enumerablePropertySource = (EnumerablePropertySource<?>) propertySource;
                for (String name : enumerablePropertySource.getPropertyNames()) {
                    properties.put(name, propertySource.getProperty(name));
                }
                result.put(propertySource.getName(), properties);
            } else {
                log.warn("Unknown PropertySource {}", propertySource);
            }
        });

        return result;
    }

}
