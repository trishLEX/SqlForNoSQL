package ru.bmstu.sqlfornosql.analyzer.parser;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.bmstu.sqlfornosql.analyzer.lexer.Scanner;
import ru.bmstu.sqlfornosql.analyzer.symbols.Symbol;
import ru.bmstu.sqlfornosql.analyzer.symbols.tokens.NumberToken;
import ru.bmstu.sqlfornosql.analyzer.symbols.tokens.Token;
import ru.bmstu.sqlfornosql.analyzer.symbols.tokens.TokenTag;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.clauses.*;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.common.ColRefListVar;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.common.ColRefVar;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.common.QualifiedNameVar;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.consts.ConstListVar;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.consts.ConstVar;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.consts.DateTimeConstVar;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.expressions.arithm.ArithmExprFactorVar;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.expressions.arithm.ArithmExprTermVar;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.expressions.arithm.ArithmExprVar;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.expressions.bool.*;
import ru.bmstu.sqlfornosql.analyzer.symbols.variables.statement.*;

import java.util.ArrayList;
import java.util.List;

public class Parser {
    private static final Logger logger = LogManager.getLogger(Parser.class);

    private Scanner scanner;
    private SelectStmtVar selectStmt;
    private Token sym;

    public Parser(Scanner scanner) {
        this.scanner = scanner;
        this.selectStmt = new SelectStmtVar();
    }

    private void parse(TokenTag tag) {
        if (sym.getTag() == tag) {
            sym = scanner.nextToken();
        } else {
            throw new IllegalStateException(tag + " expected, got " + sym);
        }
    }

    public SelectStmtVar parse() {
        sym = scanner.nextToken();
        parseSelectStmt(selectStmt);

        return selectStmt;
    }

    //SelectStmt                  ::= SELECT AllDistinctClause? TargetList?
    //                                FROM FromList WhereClause?
    //                                GroupClause? HavingClauseVar? SortClause? LimitClause? OffsetClause?
    //                                ( UnionIntOps ALL? SelectStmt )?
    private void parseSelectStmt(SelectStmtVar selectStmt) {
        selectStmt.setStart(sym.getStart());
        parse(TokenTag.SELECT);

        if (sym.getTag() == TokenTag.ALL || sym.getTag() == TokenTag.DISTINCT) {
            AllDistinctClauseVar allDistinctClause = new AllDistinctClauseVar();
            selectStmt.addSymbol(allDistinctClause);
            parseAllDistinctClause(allDistinctClause);
        }

        TargetListVar targetList = new TargetListVar();
        selectStmt.addSymbol(targetList);
        parseTargetList(targetList);

        selectStmt.addSymbol(sym);
        parse(TokenTag.FROM);

        FromListVar fromList = new FromListVar();
        selectStmt.addSymbol(fromList);
        parseFromList(fromList);
        selectStmt.setFollow(fromList.getFollow());

        if (sym.getTag() == TokenTag.WHERE) {
            WhereClauseVar whereClause = new WhereClauseVar();
            selectStmt.addSymbol(whereClause);
            parseWhereClause(whereClause);
            selectStmt.setFollow(whereClause.getFollow());
        }

        if (sym.getTag() == TokenTag.GROUP) {
            GroupByClauseVar groupByClause = new GroupByClauseVar();
            selectStmt.addSymbol(groupByClause);
            parseGroupByClause(groupByClause);
            selectStmt.setFollow(groupByClause.getFollow());
        }

        if (sym.getTag() == TokenTag.HAVING) {
            HavingClauseVar havingClause = new HavingClauseVar();
            selectStmt.addSymbol(havingClause);
            parseHavingClause(havingClause);
            selectStmt.setFollow(havingClause.getFollow());
        }

        if (sym.getTag() == TokenTag.ORDER) {
            OrderByClauseVar orderByClause = new OrderByClauseVar();
            selectStmt.addSymbol(orderByClause);
            parseOrderByClause(orderByClause);
            selectStmt.setFollow(orderByClause.getFollow());
        }

        if (sym.getTag() == TokenTag.LIMIT) {
            LimitClauseVar limitClause = new LimitClauseVar();
            selectStmt.addSymbol(limitClause);
            parseLimitClause(limitClause);
            selectStmt.setFollow(limitClause.getFollow());
        }

        if (sym.getTag() == TokenTag.OFFSET) {
            OffsetClauseVar offsetClause = new OffsetClauseVar();
            selectStmt.addSymbol(offsetClause);
            parseOffsetClause(offsetClause);
            selectStmt.setFollow(offsetClause.getFollow());
        }

        if (sym.getTag() == TokenTag.UNION
                || sym.getTag() == TokenTag.INTERSECT
                || sym.getTag() == TokenTag.EXCEPT)
        {
            UnionIntOpsVar unionIntOps = new UnionIntOpsVar();
            selectStmt.addSymbol(unionIntOps);
            parseUnionIntOps(unionIntOps);

            if (sym.getTag() == TokenTag.ALL) {
                selectStmt.addSymbol(sym);
                parse(TokenTag.ALL);
            }

            SelectStmtVar selectStmtVar = new SelectStmtVar();
            selectStmt.addSymbol(selectStmt);
            parseSelectStmt(selectStmtVar);
            selectStmt.setFollow(selectStmtVar.getFollow());
        }
    }

    //AllDistinctClause           ::= ALL
    //                            |   DISTINCT (ON '(' ColRefList ')')?
    private void parseAllDistinctClause(AllDistinctClauseVar allDistinctClause) {
        if (sym.getTag() == TokenTag.ALL) {
            allDistinctClause.addSymbol(sym);
            allDistinctClause.setCoords(sym.getCoords());
            parse(TokenTag.ALL);
        } else if (sym.getTag() == TokenTag.DISTINCT) {
            allDistinctClause.addSymbol(sym);
            allDistinctClause.setCoords(sym.getCoords());
            parse(TokenTag.DISTINCT);

            if (sym.getTag() == TokenTag.ON) {
                allDistinctClause.addSymbol(sym);
                parse(TokenTag.ON);

                allDistinctClause.addSymbol(sym);
                parse(TokenTag.LPAREN);

                ColRefListVar colRefList = new ColRefListVar();
                allDistinctClause.addSymbol(colRefList);
                parseColRefList(colRefList);

                allDistinctClause.addSymbol(sym);
                allDistinctClause.setFollow(sym.getFollow());
                parse(TokenTag.RPAREN);
            }
        } else {
            throw new IllegalStateException("ALL or DISTINCT expected, got " + sym);
        }
    }

    //ColRefList                  ::= ColRef (',' ColRef)*
    private void parseColRefList(ColRefListVar colRefList) {
        ColRefVar colRef = new ColRefVar();
        colRefList.addSymbol(colRef);
        parseColRef(colRef);
        colRefList.setCoords(colRef.getCoords());

        while (sym.getTag() == TokenTag.COMMA) {
            colRefList.addSymbol(sym);
            parse(TokenTag.COMMA);

            ColRefVar colRefVar = new ColRefVar();
            colRefList.addSymbol(colRefVar);
            parseColRef(colRefVar);
            colRefList.setFollow(colRefVar.getFollow());
        }
    }

    //ColRef                      ::= intConst //>=0 номер столбца
    //                            |   ColId
    private void parseColRef(ColRefVar colRef) {
        if (sym.getTag() == TokenTag.BYTE_CONST
                || sym.getTag() == TokenTag.SHORT_CONST
                || sym.getTag() == TokenTag.INT_CONST
                || sym.getTag() == TokenTag.LONG_CONST) {
            colRef.addSymbol(sym);
            Token number = sym;
            parseIntConst();
            Number value = ((NumberToken) number).getValue();
            if (value.intValue() <= 0) {
                throw new IllegalArgumentException("Column index is negative at " + number);
            }
        } else if (sym.getTag() == TokenTag.IDENTIFIER) {
            colRef.addSymbol(sym);
            colRef.setCoords(sym.getCoords());
            parse(TokenTag.IDENTIFIER);
        } else {
            throw new IllegalStateException("Int index or identifier expected, got " + sym);
        }
    }

    //TargetList                  ::= '*' | TargetExpr AliasClause? (',' TargetExpr AliasClause?)*
    private void parseTargetList(TargetListVar targetList) {
        if (sym.getTag() == TokenTag.MUL) {
            targetList.addSymbol(sym);
            targetList.setCoords(sym.getCoords());
            parse(TokenTag.MUL);
        } else if (sym.getTag() == TokenTag.IDENTIFIER
                || sym.getTag() == TokenTag.AVG
                || sym.getTag() == TokenTag.SUM
                || sym.getTag() == TokenTag.COUNT
                || sym.getTag() == TokenTag.MIN
                || sym.getTag() == TokenTag.MAX) {
            TargetExprVar targetExpr = new TargetExprVar();
            targetList.addSymbol(targetExpr);
            parseTargetExpr(targetExpr);
            targetList.setCoords(targetExpr.getCoords());

            if (sym.getTag() == TokenTag.AS
                    || sym.getTag() == TokenTag.IDENTIFIER) {
                AliasClauseVar aliasClause = new AliasClauseVar();
                targetList.addSymbol(aliasClause);
                parseAliasClause(aliasClause);
                targetList.setFollow(aliasClause.getFollow());
            }

            while (sym.getTag() == TokenTag.COMMA) {
                targetList.addSymbol(sym);
                parse(TokenTag.COMMA);

                TargetExprVar targetExprVar = new TargetExprVar();
                targetList.addSymbol(targetExprVar);
                parseTargetExpr(targetExpr);
                targetList.setFollow(targetExpr.getFollow());

                if (sym.getTag() == TokenTag.AS
                        || sym.getTag() == TokenTag.IDENTIFIER) {
                    AliasClauseVar aliasClause = new AliasClauseVar();
                    targetList.addSymbol(aliasClause);
                    parseAliasClause(aliasClause);
                    targetList.setFollow(aliasClause.getFollow());
                }
            }
        }
    }

    //TargetExpr                  ::= QualifiedName
    //                            |   AVG   '(' QualifiedName ')'
    //                            |   SUM   '(' QualifiedName ')'
    //                            |   COUNT '(' QualifiedName | * ')'
    //                            |   MIN   '(' QualifiedName ')'
    //                            |   MAX   '(' QualifiedName ')'
    private void parseTargetExpr(TargetExprVar targetExpr) {
        if (sym.getTag() == TokenTag.IDENTIFIER) {
            QualifiedNameVar qualifiedName = new QualifiedNameVar();
            targetExpr.addSymbol(qualifiedName);
            parseQualifiedName(qualifiedName);
            targetExpr.setCoords(qualifiedName.getCoords());
        } else if (sym.getTag() == TokenTag.AVG) {
            targetExpr.addSymbol(sym);
            targetExpr.setStart(sym.getStart());
            parse(TokenTag.AVG);

            targetExpr.addSymbol(sym);
            parse(TokenTag.LPAREN);

            QualifiedNameVar qualifiedName = new QualifiedNameVar();
            targetExpr.addSymbol(qualifiedName);
            parseQualifiedName(qualifiedName);

            targetExpr.addSymbol(sym);
            targetExpr.setFollow(sym.getFollow());
            parse(TokenTag.RPAREN);
        } else if (sym.getTag() == TokenTag.SUM) {
            targetExpr.addSymbol(sym);
            targetExpr.setStart(sym.getStart());
            parse(TokenTag.SUM);

            targetExpr.addSymbol(sym);
            parse(TokenTag.LPAREN);

            QualifiedNameVar qualifiedName = new QualifiedNameVar();
            targetExpr.addSymbol(qualifiedName);
            parseQualifiedName(qualifiedName);

            targetExpr.addSymbol(sym);
            targetExpr.setFollow(sym.getFollow());
            parse(TokenTag.RPAREN);
        } else if (sym.getTag() == TokenTag.COUNT) {
            targetExpr.addSymbol(sym);
            targetExpr.setStart(sym.getStart());
            parse(TokenTag.COUNT);

            targetExpr.addSymbol(sym);
            parse(TokenTag.LPAREN);

            if (sym.getTag() == TokenTag.IDENTIFIER) {
                QualifiedNameVar qualifiedName = new QualifiedNameVar();
                targetExpr.addSymbol(qualifiedName);
                parseQualifiedName(qualifiedName);
            } else if (sym.getTag() == TokenTag.MUL) {
                targetExpr.addSymbol(sym);
                parse(TokenTag.MUL);
            }

            targetExpr.addSymbol(sym);
            targetExpr.setFollow(sym.getFollow());
            parse(TokenTag.RPAREN);
        } else if (sym.getTag() == TokenTag.MIN) {
            targetExpr.addSymbol(sym);
            targetExpr.setStart(sym.getStart());
            parse(TokenTag.MIN);

            targetExpr.addSymbol(sym);
            parse(TokenTag.LPAREN);

            QualifiedNameVar qualifiedName = new QualifiedNameVar();
            targetExpr.addSymbol(qualifiedName);
            parseQualifiedName(qualifiedName);

            targetExpr.addSymbol(sym);
            targetExpr.setFollow(sym.getFollow());
            parse(TokenTag.RPAREN);
        } else if (sym.getTag() == TokenTag.MAX) {
            targetExpr.addSymbol(sym);
            targetExpr.setStart(sym.getStart());
            parse(TokenTag.MAX);

            targetExpr.addSymbol(sym);
            parse(TokenTag.LPAREN);

            QualifiedNameVar qualifiedName = new QualifiedNameVar();
            targetExpr.addSymbol(qualifiedName);
            parseQualifiedName(qualifiedName);

            targetExpr.addSymbol(sym);
            targetExpr.setFollow(sym.getFollow());
            parse(TokenTag.RPAREN);
        } else {
            throw new IllegalStateException("Identifier or aggregate function expected, got " + sym);
        }
    }

    //QualifiedName               ::= ColId ('.'ColId)*
    private void parseQualifiedName(QualifiedNameVar qualifiedName) {
        qualifiedName.addSymbol(sym);
        qualifiedName.setCoords(sym.getCoords());
        parse(TokenTag.IDENTIFIER);

        while (sym.getTag() == TokenTag.DOT) {
            qualifiedName.addSymbol(sym);
            parse(TokenTag.DOT);

            qualifiedName.addSymbol(sym);
            qualifiedName.setFollow(sym.getFollow());
            parse(TokenTag.IDENTIFIER);
        }
    }

    //AliasClause                 ::= AS ColId
    //                            |   ColId
    private void parseAliasClause(AliasClauseVar aliasClause) {
        if (sym.getTag() == TokenTag.AS) {
            aliasClause.addSymbol(sym);
            aliasClause.setStart(sym.getStart());
            parse(TokenTag.AS);

            aliasClause.addSymbol(sym);
            aliasClause.setFollow(sym.getFollow());
            parse(TokenTag.IDENTIFIER);
        } else if (sym.getTag() == TokenTag.IDENTIFIER) {
            aliasClause.addSymbol(sym);
            aliasClause.setCoords(sym.getCoords());
            parse(TokenTag.IDENTIFIER);
        } else {
            throw new IllegalStateException("AS or identifier expected, got " + sym);
        }
    }

    //FromList                    ::= TableRef (',' TableRef)*
    private void parseFromList(FromListVar fromList) {
        TableRefVar tableRef = new TableRefVar();
        fromList.addSymbol(tableRef);
        parseTableRef(tableRef);
        fromList.setCoords(tableRef.getCoords());

        while (sym.getTag() == TokenTag.COMMA) {
            fromList.addSymbol(sym);
            parse(TokenTag.COMMA);

            TableRefVar tableRefVar = new TableRefVar();
            fromList.addSymbol(tableRefVar);
            parseTableRef(tableRefVar);
            fromList.setFollow(tableRef.getFollow());
        }
    }

    //TableRef                    ::= QualifiedName AliasClause? ( JoinType? JOIN TableRef JoinQual )?
    private void parseTableRef(TableRefVar tableRef) {
        QualifiedNameVar qualifiedName = new QualifiedNameVar();
        tableRef.addSymbol(qualifiedName);
        parseQualifiedName(qualifiedName);
        tableRef.setCoords(qualifiedName.getCoords());

        if (sym.getTag() == TokenTag.IDENTIFIER
                || sym.getTag() == TokenTag.AS) {
            AliasClauseVar aliasClause = new AliasClauseVar();
            tableRef.addSymbol(aliasClause);
            parseAliasClause(aliasClause);
            tableRef.setFollow(aliasClause.getFollow());
        }

        if (sym.getTag() == TokenTag.FULL
                || sym.getTag() == TokenTag.LEFT
                || sym.getTag() == TokenTag.RIGHT
                || sym.getTag() == TokenTag.INNER
                || sym.getTag() == TokenTag.JOIN)
        {
            if (sym.getTag() == TokenTag.FULL
                    || sym.getTag() == TokenTag.LEFT
                    || sym.getTag() == TokenTag.RIGHT
                    || sym.getTag() == TokenTag.INNER)
            {
                JoinTypeVar joinType = new JoinTypeVar();
                tableRef.addSymbol(joinType);
                parseJoinType(joinType);
            }

            tableRef.addSymbol(sym);
            parse(TokenTag.JOIN);

            TableRefVar tableRefVar = new TableRefVar();
            tableRef.addSymbol(tableRefVar);
            parseTableRef(tableRefVar);

            JoinQualVar joinQual = new JoinQualVar();
            tableRef.addSymbol(joinQual);
            parseJoinQual(joinQual);
            tableRef.setFollow(joinQual.getFollow());
        }
    }

    //JoinType                    ::= FULL OUTER?
    //                            |   LEFT OUTER?
    //                            |   RIGHT OUTER?
    //                            |   INNER
    private void parseJoinType(JoinTypeVar joinType) {
        if (sym.getTag() == TokenTag.FULL) {
            joinType.addSymbol(sym);
            joinType.setCoords(sym.getCoords());
            parse(TokenTag.FULL);

            if (sym.getTag() == TokenTag.OUTER) {
                joinType.addSymbol(sym);
                joinType.setFollow(sym.getFollow());
                parse(TokenTag.OUTER);
            }
        } else if (sym.getTag() == TokenTag.LEFT) {
            joinType.addSymbol(sym);
            joinType.setCoords(sym.getCoords());
            parse(TokenTag.LEFT);

            if (sym.getTag() == TokenTag.OUTER) {
                joinType.addSymbol(sym);
                joinType.setFollow(sym.getFollow());
                parse(TokenTag.OUTER);
            }
        } else if (sym.getTag() == TokenTag.RIGHT) {
            joinType.addSymbol(sym);
            joinType.setCoords(sym.getCoords());
            parse(TokenTag.RIGHT);

            if (sym.getTag() == TokenTag.OUTER) {
                joinType.addSymbol(sym);
                joinType.setFollow(sym.getFollow());
                parse(TokenTag.OUTER);
            }
        } else if (sym.getTag() == TokenTag.INNER) {
            joinType.addSymbol(sym);
            joinType.setCoords(sym.getCoords());
            parse(TokenTag.INNER);
        } else {
            throw new IllegalStateException("FULL, LEFT, RIGHT or INNER expected, got " + sym);
        }
    }

    //JoinQual                    ::= USING '(' ColId (',' ColId)* ')'
    //                            |   ON BoolExpr
    private void parseJoinQual(JoinQualVar joinQual) {
        if (sym.getTag() == TokenTag.USING) {
            joinQual.addSymbol(sym);
            joinQual.setStart(sym.getStart());
            parse(TokenTag.USING);

            joinQual.addSymbol(sym);
            parse(TokenTag.LPAREN);

            joinQual.addSymbol(sym);
            parse(TokenTag.IDENTIFIER);

            while (sym.getTag() == TokenTag.COMMA) {
                joinQual.addSymbol(sym);
                parse(TokenTag.COMMA);

                joinQual.addSymbol(sym);
                parse(TokenTag.IDENTIFIER);
            }

            joinQual.addSymbol(sym);
            joinQual.setFollow(sym.getFollow());
            parse(TokenTag.RPAREN);
        }
    }

    //WhereClause                 ::= WHERE BoolExpr
    private void parseWhereClause(WhereClauseVar whereClause) {
        whereClause.addSymbol(sym);
        whereClause.setStart(sym.getStart());
        parse(TokenTag.WHERE);

        BoolExprVar boolExpr = new BoolExprVar();
        whereClause.addSymbol(boolExpr);
        parseBoolExpr(boolExpr);
        whereClause.setFollow(boolExpr.getFollow());
    }

    //BoolExpr                    ::= BoolExprTerm (OR BoolExprTerm)*
    private void parseBoolExpr(BoolExprVar boolExpr) {
        BoolExprTermVar boolExprTerm = new BoolExprTermVar();
        boolExpr.addSymbol(boolExprTerm);
        parseBoolExprTerm(boolExprTerm);
        boolExpr.setCoords(boolExprTerm.getCoords());

        while (sym.getTag() == TokenTag.OR) {
            boolExpr.addSymbol(sym);
            parse(TokenTag.OR);

            BoolExprTermVar boolExprTermVar = new BoolExprTermVar();
            boolExpr.addSymbol(boolExprTermVar);
            parseBoolExprTerm(boolExprTermVar);
            boolExpr.setFollow(boolExprTermVar.getFollow());
        }
    }

    //BoolExprTerm                ::= BoolExprFactor (AND BoolExprFactor)*
    private void parseBoolExprTerm(BoolExprTermVar boolExprTerm) {
        BoolExprFactorVar boolExprFactor = new BoolExprFactorVar();
        boolExprTerm.addSymbol(boolExprFactor);
        parseBoolExprFactor(boolExprFactor);
        boolExprTerm.setCoords(boolExprFactor.getCoords());

        while (sym.getTag() == TokenTag.AND) {
            boolExprTerm.addSymbol(sym);
            parse(TokenTag.AND);

            BoolExprFactorVar boolExprFactorVar = new BoolExprFactorVar();
            boolExprTerm.addSymbol(boolExprFactorVar);
            parseBoolExprFactor(boolExprFactorVar);
            boolExprTerm.setFollow(boolExprFactorVar.getFollow());
        }
    }

    //BoolExprFactor              ::= BoolConst BoolRHS?
    //                            |   NOT BoolExprFactor BoolRHS?
    //                            |   '(' BoolExpr ')' BoolRHS?
    //                            |   ArithmExpr RHS
    private void parseBoolExprFactor(BoolExprFactorVar boolExprFactor) {
        if (sym.getTag() == TokenTag.NOT) {
            boolExprFactor.addSymbol(sym);
            boolExprFactor.setStart(sym.getStart());
            parse(TokenTag.NOT);

            BoolExprFactorVar boolExprFactorVar = new BoolExprFactorVar();
            boolExprFactor.addSymbol(boolExprFactorVar);
            parseBoolExprFactor(boolExprFactorVar);
            boolExprFactor.setFollow(boolExprFactorVar.getFollow());
        } else if (sym.getTag() == TokenTag.LPAREN) {
            boolExprFactor.setStart(sym.getStart());
            boolExprFactor.addSymbol(sym);
            parse(TokenTag.LPAREN);

            BoolExprVar boolExpr = new BoolExprVar();
            boolExprFactor.addSymbol(boolExpr);
            parseBoolExpr(boolExpr);

            boolExprFactor.addSymbol(sym);
            boolExprFactor.setFollow(sym.getFollow());
            parse(TokenTag.RPAREN);
        } else if (sym.getTag() == TokenTag.IDENTIFIER) {
            ArithmExprVar arithmExpr = new ArithmExprVar();
            boolExprFactor.addSymbol(arithmExpr);
            parseArithmExpr(arithmExpr);
            boolExprFactor.setStart(arithmExpr.getStart());

            RhsVar rhs = new RhsVar();
            boolExprFactor.addSymbol(rhs);
            parseRhs(rhs);
            boolExprFactor.setFollow(rhs.getFollow());
        } else if (sym.getTag() == TokenTag.TRUE
                || sym.getTag() == TokenTag.FALSE
                || sym.getTag() == TokenTag.NULL){
            boolExprFactor.addSymbol(sym);
            boolExprFactor.setCoords(sym.getCoords());

            if (sym.getTag() == TokenTag.IS) {
                BoolRhsVar boolRHS = new BoolRhsVar();
                boolExprFactor.addSymbol(boolRHS);
                parseBoolRhs(boolRHS);
                boolExprFactor.setFollow(boolRHS.getFollow());
            }
        } else {
            throw new RuntimeException("Wrong symbol " + sym + " boolean expression expected");
        }
    }

    //RHS                         ::= BoolRHS | DateRHS | StringRHS | ArithmRHS | IN '(' ConstList ')'
    private void parseRhs(RhsVar rhs) {
        if (sym.getTag() == TokenTag.IS) {
            BoolRhsVar boolRhs = new BoolRhsVar();
            rhs.addSymbol(boolRhs);
            parseBoolRhs(boolRhs);
            rhs.setCoords(boolRhs.getCoords());
        } else if (sym.getTag() == TokenTag.LESS
                || sym.getTag() == TokenTag.LESSEQ
                || sym.getTag() == TokenTag.GREATER
                || sym.getTag() == TokenTag.GREATEREQ
                || sym.getTag() == TokenTag.EQUAL
                || sym.getTag() == TokenTag.NOTEQUAL)
        {
            Token first = sym;

            switch ((TokenTag) sym.getTag()) {
                case LESS:
                    parse(TokenTag.LESS);
                    break;
                case LESSEQ:
                    parse(TokenTag.LESSEQ);
                    break;
                case GREATER:
                    parse(TokenTag.GREATER);
                    break;
                case GREATEREQ:
                    parse(TokenTag.GREATEREQ);
                    break;
                case EQUAL:
                    parse(TokenTag.EQUAL);
                    break;
                case NOTEQUAL:
                    parse(TokenTag.NOTEQUAL);
                    break;
            }

            if (sym.getTag() == TokenTag.IDENTIFIER
                    || sym.getTag() == TokenTag.BYTE_CONST
                    || sym.getTag() == TokenTag.SHORT_CONST
                    || sym.getTag() == TokenTag.INT_CONST
                    || sym.getTag() == TokenTag.LONG_CONST
                    || sym.getTag() == TokenTag.FLOAT_CONST
                    || sym.getTag() == TokenTag.DOUBLE_CONST
                    || sym.getTag() == TokenTag.SUB) {
                ArithmRhsVar arithmRhs = new ArithmRhsVar();
                arithmRhs.addSymbol(first);
                arithmRhs.setStart(first.getStart());

                ArithmExprVar arithmExpr = new ArithmExprVar();
                arithmRhs.addSymbol(arithmExpr);
                parseArithmExpr(arithmExpr);
                arithmRhs.setFollow(arithmExpr.getFollow());

                rhs.addSymbol(arithmRhs);
                rhs.setCoords(arithmRhs.getCoords());
            } else if (sym.getTag() == TokenTag.STRING_CONST) {
                DateRhsVar dateRhs = new DateRhsVar();
                dateRhs.addSymbol(first);
                dateRhs.setStart(first.getStart());

                DateTimeConstVar dateTimeConst = new DateTimeConstVar();
                dateRhs.addSymbol(dateTimeConst);
                parseDateTimeConst(dateTimeConst);
                dateRhs.setFollow(dateTimeConst.getFollow());

                rhs.addSymbol(dateRhs);
                rhs.setCoords(dateRhs.getCoords());
            } else {
                throw new IllegalStateException("DateTime or arithmetic expression expected, got" + sym);
            }
        } else if (sym.getTag() == TokenTag.NOT || sym.getTag() == TokenTag.BETWEEN) {
            List<Symbol> notBetweenList = new ArrayList<>();
            if (sym.getTag() == TokenTag.NOT) {
                notBetweenList.add(sym);
                parse(TokenTag.NOT);

                notBetweenList.add(sym);
                parse(TokenTag.BETWEEN);
            } else {
                notBetweenList.add(sym);
                parse(TokenTag.BETWEEN);
            }

            if (sym.getTag() == TokenTag.IDENTIFIER
                    || sym.getTag() == TokenTag.BYTE_CONST
                    || sym.getTag() == TokenTag.SHORT_CONST
                    || sym.getTag() == TokenTag.INT_CONST
                    || sym.getTag() == TokenTag.LONG_CONST
                    || sym.getTag() == TokenTag.FLOAT_CONST
                    || sym.getTag() == TokenTag.DOUBLE_CONST
                    || sym.getTag() == TokenTag.SUB) {
                ArithmRhsVar arithmRhs = new ArithmRhsVar();
                arithmRhs.addSymbols(notBetweenList);
                arithmRhs.setStart(notBetweenList.get(0).getStart());

                ArithmExprVar arithmExpr = new ArithmExprVar();
                arithmRhs.addSymbol(arithmExpr);
                parseArithmExpr(arithmExpr);

                arithmRhs.addSymbol(sym);
                parse(TokenTag.AND);

                ArithmExprVar arithmExprVar = new ArithmExprVar();
                arithmRhs.addSymbol(arithmExprVar);
                parseArithmExpr(arithmExprVar);
                arithmRhs.setFollow(arithmExprVar.getFollow());

                rhs.addSymbol(arithmRhs);
                rhs.setCoords(arithmRhs.getCoords());
            } else if (sym.getTag() == TokenTag.STRING_CONST) {
                DateRhsVar dateRhs = new DateRhsVar();
                dateRhs.addSymbols(notBetweenList);
                dateRhs.setStart(notBetweenList.get(0).getStart());

                DateTimeConstVar dateTimeConst = new DateTimeConstVar();
                dateRhs.addSymbol(dateTimeConst);
                parseDateTimeConst(dateTimeConst);

                dateRhs.addSymbol(sym);
                parse(TokenTag.AND);

                DateTimeConstVar dateTimeConstVar = new DateTimeConstVar();
                dateRhs.addSymbol(dateTimeConstVar);
                parseDateTimeConst(dateTimeConstVar);
                dateRhs.setFollow(dateTimeConstVar.getFollow());

                rhs.addSymbol(dateRhs);
                rhs.setCoords(dateRhs.getCoords());
            }
        } else if (sym.getTag() == TokenTag.LIKE) {
            StringRhsVar stringRhs = new StringRhsVar();
            rhs.addSymbol(stringRhs);
            parseStringRhs(stringRhs);
            rhs.setCoords(stringRhs.getCoords());
        } else if (sym.getTag() == TokenTag.IN) {
            rhs.addSymbol(sym);
            rhs.setStart(sym.getStart());
            parse(TokenTag.IN);

            rhs.addSymbol(sym);
            parse(TokenTag.LPAREN);

            ConstListVar constList = new ConstListVar();
            rhs.addSymbol(constList);
            parseConstList(constList);

            rhs.addSymbol(sym);
            rhs.setFollow(sym.getFollow());
            parse(TokenTag.RPAREN);
        } else {
            throw new IllegalStateException("IS, compare operators, LIKE or IN expected, got " + sym);
        }
    }

    //BoolRHS                     ::= IS NOT? BoolConst
    private void parseBoolRhs(BoolRhsVar boolRhs) {
        boolRhs.addSymbol(sym);
        boolRhs.setStart(sym.getStart());
        parse(TokenTag.IS);

        if (sym.getTag() == TokenTag.NOT) {
            boolRhs.addSymbol(sym);
            parse(TokenTag.NOT);
        }

        boolRhs.addSymbol(sym);
        boolRhs.setFollow(sym.getFollow());
        switch ((TokenTag) sym.getTag()) {
            case TRUE:
                parse(TokenTag.TRUE);
                break;
            case FALSE:
                parse(TokenTag.FALSE);
                break;
            case NULL:
                parse(TokenTag.NULL);
                break;
            default:
                throw new IllegalStateException("TRUE, FALSE or NULL expected, got " + sym);
        }
    }

    //DateTimeConst               ::= DateValue'::'DATE
    //                            |   TimeValue'::'TIME
    //                            |   TimestampValue'::'TIMESTAMP
    private void parseDateTimeConst(DateTimeConstVar dateTimeConst) {
        dateTimeConst.addSymbol(sym);
        dateTimeConst.setStart(sym.getStart());
        parse(TokenTag.STRING_CONST);

        dateTimeConst.addSymbol(sym);
        parse(TokenTag.COLON);

        dateTimeConst.addSymbol(sym);
        parse(TokenTag.COLON);

        dateTimeConst.addSymbol(sym);
        dateTimeConst.setFollow(sym.getFollow());
        switch ((TokenTag) sym.getTag()) {
            case DATE:
                parse(TokenTag.DATE);
                break;
            case TIME:
                parse(TokenTag.TIME);
                break;
            case TIMESTAMP:
                parse(TokenTag.TIMESTAMP);
                break;
            default:
                throw new IllegalStateException("DATE, TIME or TIMESTAMP expected, got " + sym);
        }
    }

    //ConstList                   ::= Const (',' Const)*
    private void parseConstList(ConstListVar constList) {
        ConstVar constVar = new ConstVar();
        constList.addSymbol(constVar);
        parseConst(constVar);
        constList.setCoords(constVar.getCoords());

        while (sym.getTag() == TokenTag.COMMA) {
            constList.addSymbol(sym);
            parse(TokenTag.COMMA);

            ConstVar constVarEl = new ConstVar();
            constList.addSymbol(constVarEl);
            parseConst(constVarEl);
            constList.setFollow(constVarEl.getFollow());

            if (constVar.getConstType() != constVarEl.getConstType()) {
                throw new IllegalStateException("Consts have different types, " + constVarEl.getSymbols().get(0));
            }

            constVar = constVarEl;
        }
    }

    //Const                       ::= NumberConst | StringConst | BoolConst | DateTimeConst
    private void parseConst(ConstVar constVar) {
        if (sym.getTag() == TokenTag.STRING_CONST) {
            Token stringStringConst = sym;
            parse(TokenTag.STRING_CONST);

            if (sym.getTag() == TokenTag.COLON) {
                DateTimeConstVar dateTimeConst = new DateTimeConstVar();
                dateTimeConst.addSymbol(stringStringConst);
                dateTimeConst.setStart(stringStringConst.getStart());

                dateTimeConst.addSymbol(sym);
                parse(TokenTag.COLON);

                dateTimeConst.addSymbol(sym);
                parse(TokenTag.COLON);

                dateTimeConst.addSymbol(sym);
                dateTimeConst.setFollow(sym.getFollow());
                switch ((TokenTag) sym.getTag()) {
                    case DATE:
                        parse(TokenTag.DATE);
                        break;
                    case TIME:
                        parse(TokenTag.TIME);
                        break;
                    case TIMESTAMP:
                        parse(TokenTag.TIMESTAMP);
                        break;
                    default:
                        throw new IllegalStateException("DATE, TIME or TIMESTAMP expected, got " + sym);
                }

                constVar.addSymbol(dateTimeConst);
                constVar.setCoords(dateTimeConst.getCoords());
            } else {
                constVar.addSymbol(stringStringConst);
                constVar.setCoords(stringStringConst.getCoords());
            }
        } else {
            constVar.addSymbol(sym);
            constVar.setCoords(sym.getCoords());

            switch ((TokenTag) sym.getTag()) {
                case BYTE_CONST:
                    parse(TokenTag.BYTE_CONST);
                    break;
                case SHORT_CONST:
                    parse(TokenTag.SHORT_CONST);
                    break;
                case INT_CONST:
                    parse(TokenTag.INT_CONST);
                    break;
                case LONG_CONST:
                    parse(TokenTag.LONG_CONST);
                    break;
                case FLOAT_CONST:
                    parse(TokenTag.FLOAT_CONST);
                    break;
                case DOUBLE_CONST:
                    parse(TokenTag.DOUBLE_CONST);
                    break;
                case TRUE:
                    parse(TokenTag.TRUE);
                    break;
                case FALSE:
                    parse(TokenTag.FALSE);
                    break;
                case NULL:
                    parse(TokenTag.NULL);
                    break;
                default:
                    throw new IllegalStateException("Number, string, boolean or dateTime value expected, got " + sym);
            }
        }
    }

    //StringRHS                   ::= LIKE CharacterValue
    private void parseStringRhs(StringRhsVar stringRhs) {
        stringRhs.addSymbol(sym);
        stringRhs.setStart(sym.getFollow());
        parse(TokenTag.LIKE);

        stringRhs.addSymbol(sym);
        stringRhs.setFollow(sym.getFollow());
        parse(TokenTag.STRING_CONST);
    }

    //ArithmExpr                  ::= ArithmExprTerm ( {'+' | '-'} ArithmExprTerm )*
    private void parseArithmExpr(ArithmExprVar arithmExpr) {
        ArithmExprTermVar arithmExprTerm = new ArithmExprTermVar();
        arithmExpr.addSymbol(arithmExprTerm);
        parseArithmExprTerm(arithmExprTerm);
        arithmExpr.setCoords(arithmExprTerm.getCoords());

        while (sym.getTag() == TokenTag.ADD || sym.getTag() == TokenTag.SUB) {
            arithmExpr.addSymbol(sym);

            if (sym.getTag() == TokenTag.ADD) {
                parse(TokenTag.ADD);
            } else {
                parse(TokenTag.SUB);
            }

            ArithmExprTermVar arithmExprTermVar = new ArithmExprTermVar();
            arithmExpr.addSymbol(arithmExprTermVar);
            parseArithmExprTerm(arithmExprTermVar);
            arithmExpr.setFollow(arithmExprTermVar.getFollow());
        }
    }

    //ArithmExprTerm              ::= ArithmExprFactor ( {'*' | '/'} ArithmExprFactor )*
    private void parseArithmExprTerm(ArithmExprTermVar arithmExprTerm) {
        ArithmExprFactorVar arithmExprFactor = new ArithmExprFactorVar();
        arithmExprTerm.addSymbol(arithmExprFactor);
        parseArithmExprFactor(arithmExprFactor);
        arithmExprTerm.setCoords(arithmExprFactor.getCoords());

        while (sym.getTag() == TokenTag.MUL || sym.getTag() == TokenTag.DIV) {
            arithmExprTerm.addSymbol(sym);

            if (sym.getTag() == TokenTag.MUL) {
                parse(TokenTag.MUL);
            } else {
                parse(TokenTag.DIV);
            }

            ArithmExprFactorVar arithmExprFactorVar = new ArithmExprFactorVar();
            arithmExprTerm.addSymbol(arithmExprFactorVar);
            parseArithmExprFactor(arithmExprFactorVar);
            arithmExprTerm.setFollow(arithmExprFactorVar.getFollow());
        }
    }

    //ArithmExprFactor            ::= IDENT
    //                            |   NumericValue
    //                            |   '-' ArithmExprFactor
    private void parseArithmExprFactor(ArithmExprFactorVar arithmExprFactor) {
        if (sym.getTag() == TokenTag.IDENTIFIER) {
            arithmExprFactor.addSymbol(sym);
            arithmExprFactor.setCoords(sym.getCoords());
            parse(TokenTag.IDENTIFIER);
        } else if (sym.getTag() == TokenTag.BYTE_CONST
                || sym.getTag() == TokenTag.SHORT_CONST
                || sym.getTag() == TokenTag.INT_CONST
                || sym.getTag() == TokenTag.LONG_CONST
                || sym.getTag() == TokenTag.FLOAT_CONST
                || sym.getTag() == TokenTag.DOUBLE_CONST) {

            arithmExprFactor.addSymbol(sym);
            arithmExprFactor.setCoords(sym.getCoords());
            parseNumber();
        } else if (sym.getTag() == TokenTag.SUB) {
            arithmExprFactor.addSymbol(sym);
            arithmExprFactor.setStart(sym.getStart());
            parse(TokenTag.SUB);

            ArithmExprFactorVar arithmExprFactorVar = new ArithmExprFactorVar();
            arithmExprFactor.addSymbol(arithmExprFactorVar);
            parseArithmExprFactor(arithmExprFactorVar);
            arithmExprFactor.setFollow(arithmExprFactorVar.getFollow());
        } else {
            throw new RuntimeException("Number or '-' expected, got " + sym);
        }
    }

    //GroupByClause               ::= GROUP BY ColRef (',' ColRef)*
    private void parseGroupByClause(GroupByClauseVar groupByClause) {
        groupByClause.addSymbol(sym);
        groupByClause.setStart(sym.getStart());
        parse(TokenTag.GROUP);

        groupByClause.addSymbol(sym);
        parse(TokenTag.BY);

        ColRefVar colRef = new ColRefVar();
        groupByClause.addSymbol(colRef);
        parseColRef(colRef);
        groupByClause.setFollow(colRef.getFollow());

        while (sym.getTag() == TokenTag.COMMA) {
            groupByClause.addSymbol(sym);
            parse(TokenTag.COMMA);

            ColRefVar colRefVar = new ColRefVar();
            groupByClause.addSymbol(colRefVar);
            parseColRef(colRefVar);
            groupByClause.setFollow(colRefVar.getFollow());
        }
    }

    //HavingClause                ::= HAVING BoolExpr
    private void parseHavingClause(HavingClauseVar havingClause) {
        havingClause.addSymbol(sym);
        havingClause.setStart(sym.getStart());
        parse(TokenTag.HAVING);

        BoolExprVar boolExpr = new BoolExprVar();
        havingClause.addSymbol(boolExpr);
        parseBoolExpr(boolExpr);
        havingClause.setFollow(boolExpr.getFollow());
    }

    //OrderByClause               ::= ORDER BY OrderByElem (',' OrderByElem)*
    private void parseOrderByClause(OrderByClauseVar orderByClause) {
        orderByClause.addSymbol(sym);
        orderByClause.setStart(sym.getStart());
        parse(TokenTag.ORDER);

        orderByClause.addSymbol(sym);
        parse(TokenTag.BY);

        OrderByElemVar orderByElem = new OrderByElemVar();
        orderByClause.addSymbol(orderByElem);
        parseOrderByElem(orderByElem);
        orderByClause.setFollow(orderByElem.getFollow());

        while (sym.getTag() == TokenTag.COMMA) {
            orderByClause.addSymbol(sym);
            parse(TokenTag.COMMA);

            OrderByElemVar orderByElemVar = new OrderByElemVar();
            orderByClause.addSymbol(orderByElemVar);
            parseOrderByElem(orderByElemVar);
            orderByClause.setFollow(orderByElemVar.getFollow());
        }
    }

    //OrderByElem                 ::= colRef AscDesc?
    private void parseOrderByElem(OrderByElemVar orderByElem) {
        ColRefVar colRef = new ColRefVar();
        orderByElem.addSymbol(colRef);
        parseColRef(colRef);
        orderByElem.setStart(colRef.getStart());

        if (sym.getTag() == TokenTag.ASC || sym.getTag() == TokenTag.DESC) {
            AscDescVar ascDesc = new AscDescVar();
            orderByElem.addSymbol(ascDesc);
            parseAscDesc(ascDesc);
            orderByElem.setFollow(ascDesc.getFollow());
        }
    }

    //AscDesc                     ::= ASC | DESC //ASC ON DEFAULT
    private void parseAscDesc(AscDescVar ascDesc) {
        if (sym.getTag() == TokenTag.ASC) {
            ascDesc.addSymbol(sym);
            ascDesc.setCoords(sym.getCoords());
            parse(TokenTag.ASC);
        } else if (sym.getTag() == TokenTag.DESC) {
            ascDesc.addSymbol(sym);
            ascDesc.setCoords(sym.getCoords());
            parse(TokenTag.DESC);
        } else {
            throw new IllegalStateException("ASC or DESC expected, got " + sym);
        }
    }

    //LimitClause                 ::= LIMIT intConst //intConst >= 0
    private void parseLimitClause(LimitClauseVar limitClause) {
        limitClause.addSymbol(sym);
        limitClause.setStart(sym.getStart());
        parse(TokenTag.LIMIT);

        limitClause.addSymbol(sym);
        limitClause.setFollow(sym.getFollow());
        Token number = sym;
        parseIntConst();
        Number value = ((NumberToken) number).getValue();
        if (value.intValue() < 0) {
            throw new IllegalArgumentException("Limit size is negative: " + number);
        }
    }

    //OffsetClause                ::= OFFSET intConst //intConst >= 0
    private void parseOffsetClause(OffsetClauseVar offsetClause) {
        offsetClause.addSymbol(sym);
        offsetClause.setStart(sym.getStart());
        parse(TokenTag.OFFSET);

        offsetClause.addSymbol(sym);
        offsetClause.setFollow(sym.getFollow());
        Token number = sym;
        parseIntConst();
        Number value = ((NumberToken) number).getValue();
        if (value.intValue() < 0) {
            throw new IllegalArgumentException("Offset size is negative: " + number);
        }
    }

    //UnionIntOps                 ::= UNION
    //                            |   INTERSECT
    //                            |   EXCEPT
    private void parseUnionIntOps(UnionIntOpsVar unionIntOps) {
        unionIntOps.addSymbol(sym);
        unionIntOps.setCoords(sym.getCoords());

        switch ((TokenTag) sym.getTag()) {
            case UNION:
                parse(TokenTag.UNION);
                break;
            case INTERSECT:
                parse(TokenTag.INTERSECT);
                break;
            case EXCEPT:
                parse(TokenTag.EXCEPT);
                break;
            default:
                throw new IllegalStateException("UNION, INTERSECT or EXCEPT expected, got " + sym);
        }
    }

    private void parseIntConst() {
        if (sym.getTag() == TokenTag.BYTE_CONST)
            parse(TokenTag.BYTE_CONST);
        else if (sym.getTag() == TokenTag.SHORT_CONST)
            parse(TokenTag.SHORT_CONST);
        else if (sym.getTag() == TokenTag.INT_CONST)
            parse(TokenTag.INT_CONST);
        else if (sym.getTag() == TokenTag.LONG_CONST)
            parse(TokenTag.LONG_CONST);
        else
            throw new IllegalStateException("Wrong number at " + sym + " int number expected");
    }

    private void parseNumber() {
        if (sym.getTag() == TokenTag.BYTE_CONST)
            parse(TokenTag.BYTE_CONST);
        else if (sym.getTag() == TokenTag.SHORT_CONST)
            parse(TokenTag.SHORT_CONST);
        else if (sym.getTag() == TokenTag.INT_CONST)
            parse(TokenTag.INT_CONST);
        else if (sym.getTag() == TokenTag.LONG_CONST)
            parse(TokenTag.LONG_CONST);
        else if (sym.getTag() == TokenTag.FLOAT_CONST)
            parse(TokenTag.FLOAT_CONST);
        else if (sym.getTag() == TokenTag.DOUBLE_CONST)
            parse(TokenTag.DOUBLE_CONST);
        else
            throw new RuntimeException("Number expected, got " + sym);
    }
}
