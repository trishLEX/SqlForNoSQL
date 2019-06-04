package ru.bmstu.sqlfornosql;

import org.springframework.boot.test.context.TestConfiguration;
import ru.bmstu.sqlfornosql.executor.ExecutorConfiguration;

@TestConfiguration
public class FunctionalTest extends ExecutorConfiguration {

//    @Bean
//    public ExecutorConfig executorConfig() {
//        InputStream configStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("app.properties");
//        Properties properties = new Properties();
//        try {
//            properties.load(configStream);
//            return new ExecutorConfig.Builder()
//                    .setPostgresUser(properties.getProperty("postgres.user"))
//                    .setPostgresHost(properties.getProperty("postgres.host"))
//                    .setPostgresPort(Integer.valueOf(properties.getProperty("postgres.port")))
//                    .setPostgresPassword(properties.getProperty("postgres.password"))
//                    .setPostgresDatabase(properties.getProperty("postgres.database"))
//                    .setMongodbDatabase(properties.getProperty("mongodb.database"))
//                    .setMongodbCollection(properties.getProperty("mongodb.collection"))
//                    .setH2User(properties.getProperty("h2.user"))
//                    .setH2Password(properties.getProperty("h2.password", ""))
//                    .setH2Database(properties.getProperty("h2.database"))
//                    .build();
//        } catch (IOException e) {
//            throw new IllegalStateException("Can't load properties", e);
//        }
//    }
//
//    @Bean
//    @Autowired
//    public AbstractClient postgresClient(ExecutorConfig executorConfig) {
//        return new PostgresClient(
//                executorConfig.getPostgresHost(),
//                executorConfig.getPostgresPort(),
//                executorConfig.getPostgresUser(),
//                executorConfig.getPostgresPassword(),
//                executorConfig.getPostgresDatabase()
//        );
//    }
//
//    @Bean
//    @Autowired
//    public AbstractClient mongoClient(ExecutorConfig executorConfig) {
//        return new MongoClient(
//                executorConfig.getMongodbDatabase(),
//                executorConfig.getMongodbCollection()
//        );
//    }
}
