package ru.bmstu.sqlfornosql.analyzer.symbols.variables.statement;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class TableRefVar extends Var {
    public TableRefVar() {
        super(VarTag.TABLE_REF);
    }
}
