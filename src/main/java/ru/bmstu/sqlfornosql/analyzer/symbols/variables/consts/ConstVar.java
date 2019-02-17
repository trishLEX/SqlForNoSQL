package ru.bmstu.sqlfornosql.analyzer.symbols.variables.consts;

import ru.bmstu.sqlfornosql.analyzer.symbols.tokens.TokenTag;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.Var;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.VarTag;

public class ConstVar extends Var {
    public ConstVar() {
        super(VarTag.CONST);
    }

    public TokenTag getConstType() {
        if (getSymbols().get(0).getTag() != VarTag.DATE_TIME_CONST) {
            return (TokenTag) getSymbols().get(0).getTag();
        } else {
            return (TokenTag) ((Var) getSymbols().get(0)).get(0).getTag();
        }
    }
}
