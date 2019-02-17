package ru.bmstu.sqlfornosql.analyzer.symbols.variables.clauses;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class OrderByElemVar extends Var {
    public OrderByElemVar() {
        super(VarTag.ORDER_BY_ELEM);
    }
}
