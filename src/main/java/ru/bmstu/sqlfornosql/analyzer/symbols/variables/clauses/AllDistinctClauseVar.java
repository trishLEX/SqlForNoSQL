package ru.bmstu.sqlfornosql.analyzer.symbols.variables.clauses;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class AllDistinctClauseVar extends Var {
    public AllDistinctClauseVar() {
        super(VarTag.ALL_DISTINCT_CLAUSE);
    }
}
