package ru.bmstu.sqlfornosql.analyzer.symbols.tokens;

import ru.bmstu.sqlfornosql.analyzer.service.Position;

import java.time.temporal.Temporal;

public class DateTimeToken extends Token<Temporal> {
    public DateTimeToken(TokenTag tag, Position start, Position follow, Temporal value) {
        super(tag, start, follow, value);
    }
}
