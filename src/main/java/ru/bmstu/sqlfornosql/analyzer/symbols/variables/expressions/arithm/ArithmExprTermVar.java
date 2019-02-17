package ru.bmstu.sqlfornosql.analyzer.symbols.variables.expressions.arithm;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class ArithmExprTermVar extends Var {
    public ArithmExprTermVar() {
        super(VarTag.ARITHM_EXPR_TERM);
    }
}
