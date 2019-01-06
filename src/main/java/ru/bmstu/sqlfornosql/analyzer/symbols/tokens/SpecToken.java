package ru.bmstu.sqlfornosql.analyzer.symbols.tokens;

import ru.bmstu.sqlfornosql.analyzer.service.Position;

public class SpecToken extends Token<String> {
    public SpecToken(TokenTag tag, Position start, Position follow, String value) {
        super(tag, start, follow, value);
    }
}
