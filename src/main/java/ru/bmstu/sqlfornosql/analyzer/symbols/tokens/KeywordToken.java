package ru.bmstu.sqlfornosql.analyzer.symbols.tokens;

import ru.bmstu.sqlfornosql.analyzer.service.Position;

public class KeywordToken extends Token<String> {
    public KeywordToken(Position start, Position follow, TokenTag tag) {
        super(tag, start, follow, tag.toString());
    }
}
