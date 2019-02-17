package ru.bmstu.sqlfornosql.analyzer;

import org.junit.Test;
import ru.bmstu.sqlfornosql.analyzer.lexer.Scanner;
import ru.bmstu.sqlfornosql.analyzer.symbols.tokens.Token;
import ru.bmstu.sqlfornosql.analyzer.symbols.tokens.TokenTag;

public class LexerTest {
    private Scanner scanner;

    @Test
    public void dateTokenTest() {
        scanner = new Scanner("'2018-02-02'::DATE");
        Token token = scanner.nextToken();
        while (token.getTag() != TokenTag.END_OF_PROGRAM) {
            System.out.println(token);
            token = scanner.nextToken();
        }
    }
}
