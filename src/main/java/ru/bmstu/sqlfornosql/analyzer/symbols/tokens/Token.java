package ru.bmstu.sqlfornosql.analyzer.symbols.tokens;

import jdk.internal.jline.internal.Nullable;
import ru.bmstu.sqlfornosql.analyzer.service.Fragment;
import ru.bmstu.sqlfornosql.analyzer.service.Position;
import ru.bmstu.sqlfornosql.analyzer.symbols.Symbol;

public abstract class Token<T> extends Symbol {
    @Nullable
    private T value;

    public Token(TokenTag tag, Position start, Position follow, @Nullable T value) {
        super(tag, new Fragment(start, follow));
        this.value = value;
    }

    @Nullable
    public T getValue() {
        return value;
    }

    public String getStringValue() {
        return value == null ? "null" : value.toString();
    }

    @Override
    public String toString() {
        return this.getTag() + " " + getCoords() + ": " + value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || obj.getClass() != this.getClass())
            return false;

        Token other = (Token) obj;
        return this.getTag() == other.getTag() && this.value.equals(other.value);
    }

    @Override
    public int hashCode() {
        return getTag().hashCode() * 31 + value.hashCode();
    }
}
