package ru.bmstu.sqlfornosql.analyzer.lexer;

import ru.bmstu.sqlfornosql.analyzer.service.Position;

public class Message {
    private String text;
    private Position pos;

    public Message(Position pos, String text) {
        this.text = text;
        this.pos = pos;
    }
}
