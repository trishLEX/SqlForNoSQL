package ru.bmstu.sqlfornosql.analyzer.symbols.tokens;

import ru.bmstu.sqlfornosql.analyzer.service.Position;

import static ru.bmstu.sqlfornosql.analyzer.symbols.tokens.TokenTag.END_OF_PROGRAM;

public class EOFToken extends Token<Character> {
    public EOFToken(Position pos) {
        super(END_OF_PROGRAM, pos, pos, (char)0xFFFFFFFF);
    }
}
