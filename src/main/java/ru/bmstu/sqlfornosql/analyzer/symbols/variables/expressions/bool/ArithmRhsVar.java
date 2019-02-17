package ru.bmstu.sqlfornosql.analyzer.symbols.variables.expressions.bool;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class ArithmRhsVar extends Var {
    public ArithmRhsVar() {
        super(VarTag.ARITHM_RHS);
    }
}
