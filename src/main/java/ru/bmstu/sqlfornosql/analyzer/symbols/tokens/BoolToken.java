package ru.bmstu.sqlfornosql.analyzer.symbols.tokens;

import ru.bmstu.sqlfornosql.analyzer.service.Position;

public class BoolToken extends Token<Boolean> {
    public BoolToken(Position start, Position follow, Boolean value, TokenTag tag) {
        super(tag, start, follow, value);
    }
}
