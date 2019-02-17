package ru.bmstu.sqlfornosql.analyzer.symbols.variables.expressions.arithm;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class ArithmExprVar extends Var {
    public ArithmExprVar() {
        super(VarTag.ARITHM_EXPR);
    }
}
