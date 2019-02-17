package ru.bmstu.sqlfornosql.analyzer.symbols.variables.clauses;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class GroupByClauseVar extends Var {
    public GroupByClauseVar() {
        super(VarTag.GROUP_BY_CLAUSE);
    }
}
