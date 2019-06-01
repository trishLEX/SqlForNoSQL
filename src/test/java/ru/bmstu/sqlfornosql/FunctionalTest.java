package ru.bmstu.sqlfornosql;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import ru.bmstu.sqlfornosql.executor.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@TestConfiguration
public class FunctionalTest {
    private static final ExecutorConfig EXECUTOR_CONFIG;

    static {
        System.setProperty("spring.main.allow-bean-definition-overriding", "true");
        InputStream configStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("app.properties");
        Properties properties = new Properties();
        try {
            properties.load(configStream);
            EXECUTOR_CONFIG = new ExecutorConfig.Builder()
                    .setPostgresUser(properties.getProperty("postgres.user"))
                    .setPostgresHost(properties.getProperty("postgres.host"))
                    .setPostgresPort(Integer.valueOf(properties.getProperty("postgres.port")))
                    .setPostgresPassword(properties.getProperty("postgres.password"))
                    .setPostgresDatabase(properties.getProperty("postgres.database"))
                    .setMongodbDatabase(properties.getProperty("mongodb.database"))
                    .setMongodbCollection(properties.getProperty("mongodb.collection"))
                    .setH2User(properties.getProperty("h2.user"))
                    .setH2Password(properties.getProperty("h2.password", ""))
                    .setH2Database(properties.getProperty("h2.database"))
                    .build();
        } catch (IOException e) {
            throw new IllegalStateException("Can't load properties", e);
        }
    }

    @Bean
    public Executor executor() {
        return new Executor(EXECUTOR_CONFIG);
    }

    @Bean
    public Orderer orderer() {
        return new Orderer(EXECUTOR_CONFIG);
    }

    @Bean
    public Joiner joiner() {
        return new Joiner();
    }

    @Bean
    public Grouper grouper() {
        return new Grouper(EXECUTOR_CONFIG);
    }
}
