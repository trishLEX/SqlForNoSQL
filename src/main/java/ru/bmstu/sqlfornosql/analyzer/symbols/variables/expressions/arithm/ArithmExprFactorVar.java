package ru.bmstu.sqlfornosql.analyzer.symbols.variables.expressions.arithm;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class ArithmExprFactorVar extends Var {
    public ArithmExprFactorVar() {
        super(VarTag.ARITHM_EXPR_FACTOR);
    }
}
