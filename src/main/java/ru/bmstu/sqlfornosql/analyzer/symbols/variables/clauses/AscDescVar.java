package ru.bmstu.sqlfornosql.analyzer.symbols.variables.clauses;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class AscDescVar extends Var {
    public AscDescVar() {
        super(VarTag.ASC_DESC);
    }
}
