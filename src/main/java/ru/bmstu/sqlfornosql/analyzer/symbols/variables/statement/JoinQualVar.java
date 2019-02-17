package ru.bmstu.sqlfornosql.analyzer.symbols.variables.statement;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class JoinQualVar extends Var {
    public JoinQualVar() {
        super(VarTag.JOIN_QUAL);
    }
}
