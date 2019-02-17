package ru.bmstu.sqlfornosql.analyzer.symbols.variables.consts;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class TimeConstVar extends Var {
    public TimeConstVar() {
        super(VarTag.TIME_CONST);
    }
}
