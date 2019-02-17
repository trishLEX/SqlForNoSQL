package ru.bmstu.sqlfornosql.analyzer.symbols.variables.statement;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class TargetExprVar extends Var {
    public TargetExprVar() {
        super(VarTag.TARGET_EXPR);
    }
}
