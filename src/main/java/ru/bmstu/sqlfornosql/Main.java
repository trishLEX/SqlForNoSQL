package ru.bmstu.sqlfornosql;

import ru.bmstu.sqlfornosql.executor.Executor;
import ru.bmstu.sqlfornosql.model.Table;

import java.util.Iterator;

public class Main {
    public static void main(String[] args) {
        String query = args[0];
        Executor executor = new Executor();
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
