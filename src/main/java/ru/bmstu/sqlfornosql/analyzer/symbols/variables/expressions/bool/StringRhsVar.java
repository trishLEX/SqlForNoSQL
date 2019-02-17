package ru.bmstu.sqlfornosql.analyzer.symbols.variables.expressions.bool;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class StringRhsVar extends Var {
    public StringRhsVar() {
        super(VarTag.STRING_RHS);
    }
}
