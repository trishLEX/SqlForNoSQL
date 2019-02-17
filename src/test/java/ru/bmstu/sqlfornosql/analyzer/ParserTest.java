package ru.bmstu.sqlfornosql.analyzer;

import org.junit.Test;
import ru.bmstu.sqlfornosql.analyzer.lexer.Scanner;
import ru.bmstu.sqlfornosql.analyzer.parser.Parser;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.statement.SelectStmtVar;

public class ParserTest {
    private Parser parser;

    @Test
    public void simpleSelectTest() {
        parser = new Parser(new Scanner("SELECT * FROM postgres.db.schema.table"));
        SelectStmtVar selectStmt = parser.parse();
        System.out.println(selectStmt);
    }

    @Test
    public void whereSelectTest() {
        parser = new Parser(new Scanner("SELECT a FROM table WHERE a * 2 >= 10 + 2"));
        SelectStmtVar selectStmt = parser.parse();
        System.out.println(selectStmt);
    }

    @Test
    public void whereDateSelectTest() {
        parser = new Parser(new Scanner("SELECT a FROM table WHERE a BETWEEN '17:28:00'::TIME AND '17:29:00'::TIME"));
        SelectStmtVar selectStmt = parser.parse();
        System.out.println(selectStmt);
    }

    @Test
    public void whereGroupBySelectTest() {
        parser = new Parser(new Scanner("SELECT MAX(b) FROM db.\"table\" WHERE a LIKE '%s' GROUP BY a"));
        SelectStmtVar selectStmt = parser.parse();
        System.out.println(selectStmt);
    }

    @Test
    public void whereHavingGroupByTest() {
        parser = new Parser(new Scanner("SELECT MAX(b) FROM db.\"table\" WHERE a LIKE '%s' GROUP BY a HAVING b > 100"));
        SelectStmtVar selectStmt = parser.parse();
        System.out.println(selectStmt);
    }

    @Test
    public void whereOrderByLimitOffsetTest() {
        parser = new Parser(new Scanner(
                "SELECT MAX(b) FROM db.\"table\" WHERE a LIKE '%s' GROUP BY a HAVING b IS NULL ORDER BY b ASC LIMIT 5 OFFSET 1"
        ));
        SelectStmtVar selectStmt = parser.parse();
        System.out.println(selectStmt);
    }

    @Test
    public void whereInTest() {
        parser = new Parser(new Scanner("SELECT DISTINCT a AS COL FROM db.table WHERE b IN (1, 2, 3)"));
        SelectStmtVar selectStmt = parser.parse();
        System.out.println(selectStmt);
    }
}
