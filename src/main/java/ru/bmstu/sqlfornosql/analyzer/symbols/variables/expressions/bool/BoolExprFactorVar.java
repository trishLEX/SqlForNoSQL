package ru.bmstu.sqlfornosql.analyzer.symbols.variables.expressions.bool;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class BoolExprFactorVar extends Var {
    public BoolExprFactorVar() {
        super(VarTag.BOOL_EXPR_FACTOR);
    }
}
