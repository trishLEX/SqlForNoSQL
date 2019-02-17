package ru.bmstu.sqlfornosql.analyzer.symbols.variables.common;

import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class QualifiedNameVar extends Var {
    public QualifiedNameVar() {
        super(VarTag.QUALIFIED_NAME);
    }
}
