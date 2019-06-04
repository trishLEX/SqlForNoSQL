package ru.bmstu.sqlfornosql.executor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.bmstu.sqlfornosql.adapters.AbstractClient;
import ru.bmstu.sqlfornosql.adapters.mongo.MongoClient;
import ru.bmstu.sqlfornosql.adapters.postgres.PostgresClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Configuration
public class ExecutorConfiguration {
    @Bean
    public ExecutorConfig executorConfig() {
        InputStream configStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("app.properties");
        Properties properties = new Properties();
        try {
            properties.load(configStream);
            return new ExecutorConfig.Builder()
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
    @Autowired
    public AbstractClient postgresClient(ExecutorConfig executorConfig) {
        return new PostgresClient(
                executorConfig.getPostgresHost(),
                executorConfig.getPostgresPort(),
                executorConfig.getPostgresUser(),
                executorConfig.getPostgresPassword(),
                executorConfig.getPostgresDatabase()
         );
    }

    @Bean
    @Autowired
    public AbstractClient mongoClient(ExecutorConfig executorConfig) {
        return new MongoClient(
                executorConfig.getMongodbDatabase(),
                executorConfig.getMongodbCollection()
        );
    }

    @Bean
    @Autowired
    public Executor executor(
            Orderer orderer,
            Joiner joiner,
            Grouper grouper,
            AbstractClient postgresClient,
            AbstractClient mongoClient
    ) {
        return new Executor(orderer, joiner, grouper, postgresClient, mongoClient);
    }

    @Bean
    @Autowired
    public Orderer orderer(ExecutorConfig executorConfig) {
        return new Orderer(executorConfig);
    }

    @Bean
    public Joiner joiner() {
        return new Joiner();
    }

    @Bean
    @Autowired
    public Grouper grouper(ExecutorConfig executorConfig) {
        return new Grouper(executorConfig);
    }
}
