package ru.bmstu.sqlfornosql.analyzer.symbols.variables.clauses;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class OrderByClauseVar extends Var {
    public OrderByClauseVar() {
        super(VarTag.ORDER_BY_CLAUSE);
    }
}
