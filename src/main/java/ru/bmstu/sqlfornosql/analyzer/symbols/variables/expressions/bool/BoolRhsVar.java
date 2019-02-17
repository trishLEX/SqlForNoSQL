package ru.bmstu.sqlfornosql.analyzer.symbols.variables.expressions.bool;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class BoolRhsVar extends Var {
    public BoolRhsVar() {
        super(VarTag.BOOL_RHS);
    }
}
