package ru.bmstu.sqlfornosql.analyzer.symbols.variables.expressions.bool;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class BoolExprTermVar extends Var {
    public BoolExprTermVar() {
        super(VarTag.BOOL_EXPR_TERM);
    }
}
