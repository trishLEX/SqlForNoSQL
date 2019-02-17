package ru.bmstu.sqlfornosql.analyzer.symbols.variables.expressions.bool;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class DateRhsVar extends Var {
    public DateRhsVar() {
        super(VarTag.DATE_RHS);
    }
}
