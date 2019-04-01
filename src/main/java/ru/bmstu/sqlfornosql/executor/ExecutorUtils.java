package ru.bmstu.sqlfornosql.executor;

import net.sf.jsqlparser.expression.Expression;
import org.medfoster.sqljep.ParseException;
import org.medfoster.sqljep.RowJEP;
import ru.bmstu.sqlfornosql.model.Row;

import java.util.HashMap;
import java.util.regex.Matcher;

import static ru.bmstu.sqlfornosql.executor.Executor.FORBIDDEN_STRINGS;
import static ru.bmstu.sqlfornosql.executor.Executor.IDENT_REGEXP;

public class ExecutorUtils {
    public static HashMap<String, Integer> getIdentMapping(String expression) {
        Matcher matcher = IDENT_REGEXP.matcher(expression.replaceAll("'.*'", ""));
        HashMap<String, Integer> mapping = new HashMap<>();
        int index = 0;
        while (matcher.find()) {
            if (!FORBIDDEN_STRINGS.contains(matcher.group(1).toUpperCase())) {
                mapping.put(matcher.group(1).toLowerCase(), index++);
            }
        }

        return mapping;
    }

    public static RowJEP prepareSqlJEP(Expression expression, HashMap<String, Integer> colMapping) {
        RowJEP sqljep = new RowJEP(expression.toString().toLowerCase());
        try {
            sqljep.parseExpression(colMapping);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Can't parse expression: " + expression, e);
        }

        return sqljep;
    }

    public static Comparable getValue(Row row, String key) {
        return (Comparable) row.getObject(key);
    }

    public static Comparable getValueKeyIgnoreCase(Row row, String key) {
        return (Comparable) row.getObject(key.toLowerCase());
    }
}
