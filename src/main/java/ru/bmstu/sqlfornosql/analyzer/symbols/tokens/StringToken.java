package ru.bmstu.sqlfornosql.analyzer.symbols.tokens;

import ru.bmstu.sqlfornosql.analyzer.service.Position;

public class StringToken extends Token<String> {
    public StringToken(Position start, Position follow, String value) {
        super(TokenTag.STRING_CONST, start, follow, value);
    }
}
