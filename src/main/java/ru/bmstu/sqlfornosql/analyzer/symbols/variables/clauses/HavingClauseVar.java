package ru.bmstu.sqlfornosql.analyzer.symbols.variables.clauses;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class HavingClauseVar extends Var {
    public HavingClauseVar() {
        super(VarTag.HAVING_CLAUSE);
    }
}
