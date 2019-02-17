package ru.bmstu.sqlfornosql.analyzer.symbols.variables.clauses;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class LimitClauseVar extends Var {
    public LimitClauseVar() {
        super(VarTag.LIMIT_CLAUSE);
    }
}
