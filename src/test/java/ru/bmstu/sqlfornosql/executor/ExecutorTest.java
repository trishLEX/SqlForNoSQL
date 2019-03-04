package ru.bmstu.sqlfornosql.executor;

import java.util.regex.Matcher;

import org.junit.Test;

public class ExecutorTest {
    @Test
    public void testIdentMatcher() {
        String expression = "table1.a = table2.b AND table1.c = table1.d AND table.str LIKE 'hello'"
                .replaceAll("'.*'", "");
        Matcher matcher = Executor.IDENT_REGEXP.matcher(expression);
        while (matcher.find()) {
            if (!Executor.FORBIDDEN_STRINGS.contains(matcher.group(1))) {
                System.out.println(matcher.group(1));
            }
        }
    }
}
