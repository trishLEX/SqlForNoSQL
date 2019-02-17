package ru.bmstu.sqlfornosql.analyzer.symbols.variables.expressions.bool;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class BoolExprVar extends Var {
    public BoolExprVar() {
        super(VarTag.BOOL_EXPR);
    }
}
