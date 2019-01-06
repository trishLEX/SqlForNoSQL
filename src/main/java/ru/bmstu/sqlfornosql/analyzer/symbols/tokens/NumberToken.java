package ru.bmstu.sqlfornosql.analyzer.symbols.tokens;

import ru.bmstu.sqlfornosql.analyzer.service.Position;

public class NumberToken extends Token<Number> {
    public NumberToken(TokenTag tag, Position start, Position follow, Number value) {
        super(tag, start, follow, value);
    }
}
