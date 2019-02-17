package ru.bmstu.sqlfornosql.analyzer.symbols.variables.statement;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class SelectStmtVar extends Var {
    public SelectStmtVar() {
        super(VarTag.SELECT_STMT);
    }
}
