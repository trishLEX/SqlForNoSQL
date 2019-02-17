package ru.bmstu.sqlfornosql.analyzer.symbols.variables.common;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class ColRefListVar extends Var {
    public ColRefListVar() {
        super(VarTag.COL_REF_LIST);
    }
}
