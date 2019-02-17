package ru.bmstu.sqlfornosql.analyzer.symbols.variables.common;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class ColRefVar extends Var {
    public ColRefVar() {
        super(VarTag.COL_REF);
    }
}
