package ru.bmstu.sqlfornosql;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import ru.bmstu.sqlfornosql.executor.Executor;
import ru.bmstu.sqlfornosql.model.Table;

import java.util.Iterator;

@SpringBootApplication
public class Main implements CommandLineRunner {
    @Autowired
    private Executor executor;

    public static void main(String[] args) {
        System.setProperty("spring.main.allow-bean-definition-overriding", "true");
        SpringApplication.run(Main.class, args);
    }

    @Override
    public void run(String[] args) {
        String query = args[0];
        printResult(executor.execute(query));
    }

    private static void printResult(Iterator<Table> table) {
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }
}
