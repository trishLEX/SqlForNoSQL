package ru.bmstu.sqlfornosql.analyzer.symbols.variables.statement;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class TargetListVar extends Var {
    public TargetListVar() {
        super(VarTag.TARGET_LIST);
    }
}
