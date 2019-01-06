package ru.bmstu.sqlfornosql.analyzer.symbols.tokens;

import ru.bmstu.sqlfornosql.analyzer.service.Position;

public class IdentToken extends Token<String> {
    public IdentToken(Position start, Position follow, String value) {
        super(TokenTag.IDENTIFIER, start, follow, value);
    }
}
