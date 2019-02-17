package ru.bmstu.sqlfornosql.analyzer.symbols.variables.consts;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class DateConstVar extends Var {
    public DateConstVar() {
        super(VarTag.DATE_CONST);
    }
}
