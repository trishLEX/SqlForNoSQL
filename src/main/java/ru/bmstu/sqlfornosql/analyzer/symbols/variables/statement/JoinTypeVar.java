package ru.bmstu.sqlfornosql.analyzer.symbols.variables.statement;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class JoinTypeVar extends Var {
    public JoinTypeVar() {
        super(VarTag.JOIN_TYPE);
    }
}
