package ru.bmstu.sqlfornosql;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.SelectUtils;
import org.junit.Test;

public class JSqlParserTest {
    @Test
    public void selectTest() throws JSQLParserException {
        Select select = SelectUtils.buildSelectFromTable(new Table("postgresDB.postgresCatalog.schema.table"));
        Select select1 = (Select) CCJSqlParserUtil.parse("SELECT DISTINCT a, b FROM postgresDB.postgresCatalog.schema.table WHERE a = '2018-01-01'::DATE");
        System.out.println(select1);
        PlainSelect body = (PlainSelect) select1.getSelectBody();
        //select1.getSelectBody().
        System.out.println(body.getFromItem());
        System.out.println(body.getDistinct());
        System.out.println(body.getWhere());
        System.out.println(body.getSelectItems());
    }

    @Test
    public void selectJoinTest() throws JSQLParserException {
        Select select = SelectUtils.buildSelectFromTable(new Table("postgresDB.postgresCatalog.schema.table"));
        Select select1 = (Select) CCJSqlParserUtil.parse("SELECT a FROM (SELECT DISTINCT a, b FROM postgresDB.postgresCatalog.schema.table JOIN mongodb.test.test ON a = b) " +
                "JOIN postgres.db.schema.table ON a = b");
        System.out.println(select1);
        PlainSelect body = (PlainSelect) select1.getSelectBody();
        //select1.getSelectBody().
        System.out.println(body.getFromItem());
        System.out.println(body.getDistinct());
        System.out.println(body.getWhere());
        System.out.println(body.getSelectItems());
    }
}
