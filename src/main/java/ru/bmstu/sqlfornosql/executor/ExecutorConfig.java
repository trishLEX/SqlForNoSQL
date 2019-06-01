package ru.bmstu.sqlfornosql.executor;

public class ExecutorConfig {
    private String postgresUser;
    private String postgresHost;
    private String postgresPassword;
    private String postgresDatabase;
    private int postgresPort;

    private String mongodbDatabase;
    private String mongodbCollection;

    private String h2User;
    private String h2Password;
    private String h2Database;

    private ExecutorConfig(ExecutorConfig.Builder builder) {
        this.postgresUser = builder.postgresUser;
        this.postgresHost = builder.postgresHost;
        this.postgresPort = builder.postgresPort;
        this.postgresPassword = builder.postgresPassword;
        this.postgresDatabase = builder.postgresDatabase;

        this.mongodbDatabase = builder.mongodbDatabase;
        this.mongodbCollection = builder.mongodbCollection;

        this.h2User = builder.h2User;
        this.h2Password = builder.h2Password;
        this.h2Database = builder.h2Database;
    }

    public String getPostgresUser() {
        return postgresUser;
    }

    public String getPostgresHost() {
        return postgresHost;
    }

    public int getPostgresPort() {
        return postgresPort;
    }

    public String getPostgresPassword() {
        return postgresPassword;
    }

    public String getPostgresDatabase() {
        return postgresDatabase;
    }

    public String getMongodbDatabase() {
        return mongodbDatabase;
    }

    public String getMongodbCollection() {
        return mongodbCollection;
    }

    public String getH2User() {
        return h2User;
    }

    public String getH2Password() {
        return h2Password;
    }

    public String getH2Database() {
        return h2Database;
    }

    public static class Builder {
        private String postgresUser;
        private String postgresHost;
        private String postgresPassword;
        private String postgresDatabase;
        private int postgresPort;

        private String mongodbDatabase;
        private String mongodbCollection;

        private String h2User;
        private String h2Password;
        private String h2Database;

        public Builder setPostgresUser(String postgresUser) {
            this.postgresUser = postgresUser;
            return this;
        }

        public Builder setPostgresHost(String postgresHost) {
            this.postgresHost = postgresHost;
            return this;
        }

        public Builder setPostgresPassword(String postgresPassword) {
            this.postgresPassword = postgresPassword;
            return this;
        }

        public Builder setPostgresPort(int postgresPort) {
            this.postgresPort = postgresPort;
            return this;
        }

        public Builder setPostgresDatabase(String postgresDatabase) {
            this.postgresDatabase = postgresDatabase;
            return this;
        }

        public Builder setMongodbDatabase(String mongodbDatabase) {
            this.mongodbDatabase = mongodbDatabase;
            return this;
        }

        public Builder setMongodbCollection(String mongodbCollection) {
            this.mongodbCollection = mongodbCollection;
            return this;
        }

        public Builder setH2User(String h2User) {
            this.h2User = h2User;
            return this;
        }

        public Builder setH2Password(String h2Password) {
            this.h2Password = h2Password;
            return this;
        }

        public Builder setH2Database(String h2Database) {
            this.h2Database = h2Database;
            return this;
        }

        public ExecutorConfig build() {
            return new ExecutorConfig(this);
        }
    }
}
