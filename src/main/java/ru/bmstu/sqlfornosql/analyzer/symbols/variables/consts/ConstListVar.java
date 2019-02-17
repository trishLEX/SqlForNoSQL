package ru.bmstu.sqlfornosql.analyzer.symbols.variables.consts;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class ConstListVar extends Var {
    public ConstListVar() {
        super(VarTag.CONST_LIST);
    }
}
