package ru.bmstu.sqlfornosql.analyzer.symbols.variables.clauses;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class AliasClauseVar extends Var {
    public AliasClauseVar() {
        super(VarTag.ALIAS_CLAUSE);
    }
}
