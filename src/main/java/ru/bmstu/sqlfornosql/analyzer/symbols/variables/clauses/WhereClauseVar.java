package ru.bmstu.sqlfornosql.analyzer.symbols.variables.clauses;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class WhereClauseVar extends Var {
    public WhereClauseVar() {
        super(VarTag.WHERE_CLAUSE);
    }
}
