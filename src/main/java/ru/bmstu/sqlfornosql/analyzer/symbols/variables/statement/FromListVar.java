package ru.bmstu.sqlfornosql.analyzer.symbols.variables.statement;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class FromListVar extends Var {
    public FromListVar() {
        super(VarTag.FROM_LIST);
    }
}
