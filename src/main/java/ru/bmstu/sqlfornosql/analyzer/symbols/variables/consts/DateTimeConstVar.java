package ru.bmstu.sqlfornosql.analyzer.symbols.variables.consts;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class DateTimeConstVar extends Var {
    public DateTimeConstVar() {
        super(VarTag.DATE_TIME_CONST);
    }
}
