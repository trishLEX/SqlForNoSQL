package ru.bmstu.sqlfornosql.analyzer.symbols.variables.statement;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class UnionIntOpsVar extends Var {
    public UnionIntOpsVar() {
        super(VarTag.UNION_INT_OPS);
    }
}
