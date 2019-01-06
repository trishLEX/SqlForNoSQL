package ru.bmstu.sqlfornosql.analyzer.lexer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.bmstu.sqlfornosql.analyzer.symbols.tokens.*;
import ru.bmstu.sqlfornosql.analyzer.service.Position;

import java.util.ArrayList;
import java.util.List;

public class Scanner {
    private static final Logger logger = LogManager.getLogger(Scanner.class);

    private Position cur;
    private List<Message> messages;

    public Scanner(Position cur) {
        this.cur = cur;
        this.messages = new ArrayList<>();
    }

    public Token nextToken() {
        while (!cur.isEOF()) {
            while (cur.isWhiteSpace()) {
                cur.nextCp();
            }

            Position start = cur.copy();
            StringBuilder value;

            switch (cur.getChar()) {
                case 'a':
                    value = new StringBuilder("a");
                    cur.nextCp();
                    if (cur.getChar() == 'l') {
                        value.append("l");
                        cur.nextCp();
                        if (cur.getChar() == 'l') {
                            value.append("l");
                            cur.nextCp();
                            if (cur.isWhiteSpace() || cur.isSpecial()) {
                                return new KeywordToken(start, cur.copy(), TokenTag.ALL);
                            } else {
                                return getIdent(start, value);
                            }
                        }
                    } else if (cur.getChar() == 'n') {
                        value.append("n");
                        cur.nextCp();
                        if (cur.getChar() == 'd') {
                            value.append("d");
                            cur.nextCp();
                            if (cur.isWhiteSpace() || cur.isSpecial()) {
                                return new KeywordToken(start, cur.copy(), TokenTag.AND);
                            } else {
                                return getIdent(start, value);
                            }
                        }
                    } else if (cur.getChar() == 's') {
                        value.append("s");
                        cur.nextCp();
                        if (cur.getChar() == 'c') {
                            value.append("c");
                            cur.nextCp();
                            if (cur.isWhiteSpace() || cur.isSpecial()) {
                                return new KeywordToken(start, cur.copy(), TokenTag.ASC);
                            } else {
                                return getIdent(start, value);
                            }
                        } else if (cur.isWhiteSpace() || cur.isSpecial()) {
                            return new KeywordToken(start, cur.copy(), TokenTag.AS);
                        } else {
                            return getIdent(start, value);
                        }
                    } else if (cur.getChar() == 'v') {
                        value.append("v");
                        cur.nextCp();
                        if (cur.getChar() == 'g') {
                            value.append("g");
                            cur.nextCp();
                            if (cur.isWhiteSpace() || cur.isSpecial()) {
                                return new KeywordToken(start, cur.copy(), TokenTag.AVG);
                            } else {
                                return getIdent(start, value);
                            }
                        }
                    }

                    return getIdent(start, value);

                case 'b':
                    value = new StringBuilder("b");
                    cur.nextCp();
                    if (cur.getChar() == 'e') {
                        value.append("e");
                        cur.nextCp();
                        if (cur.getChar() == 't') {
                            value.append("t");
                            cur.nextCp();
                            if (cur.getChar() == 'w') {
                                value.append("w");
                                cur.nextCp();
                                if (cur.getChar() == 'e') {
                                    value.append("e");
                                    cur.nextCp();
                                    if (cur.getChar() == 'e') {
                                        value.append("e");
                                        cur.nextCp();
                                        if (cur.getChar() == 'n') {
                                            value.append("n");
                                            cur.nextCp();
                                            if (cur.isWhiteSpace() || cur.isSpecial()) {
                                                return new KeywordToken(start, cur.copy(), TokenTag.BETWEEN);
                                            } else {
                                                return getIdent(start, value);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else if (cur.getChar() == 'y') {
                        value.append("y");
                        cur.nextCp();
                        if (cur.isWhiteSpace() || cur.isSpecial()) {
                            return new KeywordToken(start, cur.copy(), TokenTag.BY);
                        } else {
                            return getIdent(start, value);
                        }
                    }

                    return getIdent(start, value);

                case 'c':
                    value = new StringBuilder("c");
                    cur.nextCp();
                    if (cur.getChar() == 'o') {
                        value.append("o");
                        cur.nextCp();
                        if (cur.getChar() == 'u') {
                            value.append("u");
                            cur.nextCp();
                            if (cur.getChar() == 'n') {
                                value.append("n");
                                cur.nextCp();
                                if (cur.getChar() == 't') {
                                    value.append("t");
                                    cur.nextCp();
                                    if (cur.isWhiteSpace() || cur.isSpecial()) {
                                        return new KeywordToken(start, cur.copy(), TokenTag.COUNT);
                                    } else {
                                        return getIdent(start, value);
                                    }
                                }
                            }
                        }
                    }

                    return getIdent(start, value);

                case 'd':
                    value = new StringBuilder("d");
                    cur.nextCp();
                    if (cur.getChar() == 'e') {
                        value.append("e");
                        cur.nextCp();
                        if (cur.getChar() == 's') {
                            value.append("s");
                            cur.nextCp();
                            if (cur.getChar() == 'c') {
                                value.append("c");
                                cur.nextCp();
                                if (cur.isWhiteSpace() || cur.isSpecial()) {
                                    return new KeywordToken(start, cur.copy(), TokenTag.DESC);
                                } else {
                                    return getIdent(start, value);
                                }
                            }
                        }
                    } else if (cur.getChar() == 'i') {
                        value.append("i");
                        cur.nextCp();
                        if (cur.getChar() == 's') {
                            value.append("s");
                            cur.nextCp();
                            if (cur.getChar() == 't') {
                                value.append("t");
                                cur.nextCp();
                                if (cur.getChar() == 'i') {
                                    value.append("i");
                                    cur.nextCp();
                                    if (cur.getChar() == 'n') {
                                        value.append("n");
                                        cur.nextCp();
                                        if (cur.getChar() == 'c') {
                                            value.append("c");
                                            cur.nextCp();
                                            if (cur.getChar() == 't') {
                                                value.append("t");
                                                cur.nextCp();
                                                if (cur.isWhiteSpace() || cur.isSpecial()) {
                                                    return new KeywordToken(start, cur.copy(), TokenTag.DISTINCT);
                                                } else {
                                                    return getIdent(start, value);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    return getIdent(start, value);

                case 'e':
                    value = new StringBuilder("e");
                    cur.nextCp();
                    if (cur.getChar() == 'x') {
                        value.append("x");
                        cur.nextCp();
                        if (cur.getChar() == 'c') {
                            value.append("c");
                            cur.nextCp();
                            if (cur.getChar() == 'e') {
                                value.append("e");
                                cur.nextCp();
                                if (cur.getChar() == 'p') {
                                    value.append("p");
                                    cur.nextCp();
                                    if (cur.getChar() == 't') {
                                        value.append("t");
                                        cur.nextCp();
                                        if (cur.isWhiteSpace() || cur.isSpecial()) {
                                            return new KeywordToken(start, cur.copy(), TokenTag.EXCEPT);
                                        } else {
                                            return getIdent(start, value);
                                        }
                                    }
                                }
                            }
                        } else if (cur.getChar() == 'i') {
                            value.append("i");
                            cur.nextCp();
                            if (cur.getChar() == 's') {
                                value.append("s");
                                cur.nextCp();
                                if (cur.getChar() == 't') {
                                    value.append("t");
                                    cur.nextCp();
                                    if (cur.getChar() == 's') {
                                        value.append("s");
                                        cur.nextCp();
                                        if (cur.isWhiteSpace() || cur.isSpecial()) {
                                            return new KeywordToken(start, cur.copy(), TokenTag.EXISTS);
                                        } else {
                                            return getIdent(start, value);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    getIdent(start, value);

                case 'f':
                    value = new StringBuilder("f");
                    cur.nextCp();
                    if (cur.getChar() == 'a') {
                        value.append("a");
                        cur.nextCp();
                        if (cur.getChar() == 'l') {
                            value.append("l");
                            cur.nextCp();
                            if (cur.getChar() == 's') {
                                value.append("s");
                                cur.nextCp();
                                if (cur.getChar() == 'e') {
                                    value.append("e");
                                    cur.nextCp();
                                    if (cur.isWhiteSpace() || cur.isSpecial()) {
                                        return new BoolToken(start, cur.copy(), false, TokenTag.FALSE);
                                    } else {
                                        return getIdent(start, value);
                                    }
                                }
                            }
                        }
                    } else if (cur.getChar() == 'r') {
                        value.append("r");
                        cur.nextCp();
                        if (cur.getChar() == 'o') {
                            value.append("o");
                            cur.nextCp();
                            if (cur.getChar() == 'm') {
                                value.append("m");
                                cur.nextCp();
                                if (cur.isWhiteSpace() || cur.isSpecial()) {
                                    return new KeywordToken(start, cur.copy(), TokenTag.FROM);
                                } else {
                                    return getIdent(start, value);
                                }
                            }
                        }
                    } else if (cur.getChar() == 'u') {
                        value.append("u");
                        cur.nextCp();
                        if (cur.getChar() == 'l') {
                            value.append("l");
                            cur.nextCp();
                            if (cur.getChar() == 'l') {
                                value.append("l");
                                cur.nextCp();
                                if (cur.isWhiteSpace() || cur.isSpecial()) {
                                    return new KeywordToken(start, cur.copy(), TokenTag.FULL);
                                } else {
                                    return getIdent(start, value);
                                }
                            }
                        }
                    }

                    getIdent(start, value);

                case 'g':
                    value = new StringBuilder("g");
                    cur.nextCp();
                    if (cur.getChar() == 'r') {
                        value.append("r");
                        cur.nextCp();
                        if (cur.getChar() == 'o') {
                            value.append('o');
                            cur.nextCp();
                            if (cur.getChar() == 'u') {
                                value.append("u");
                                cur.nextCp();
                                if (cur.getChar() == 'p') {
                                    value.append("p");
                                    cur.nextCp();
                                    if (cur.isWhiteSpace() || cur.isSpecial()) {
                                        return new KeywordToken(start, cur.copy(), TokenTag.GROUP);
                                    } else {
                                        return getIdent(start, value);
                                    }
                                }
                            }
                        }
                    }

                    return getIdent(start, value);

                case 'h':
                    value = new StringBuilder("h");
                    cur.nextCp();
                    if (cur.getChar() == 'a') {
                        value.append("a");
                        cur.nextCp();
                        if (cur.getChar() == 'v') {
                            value.append("v");
                            cur.nextCp();
                            if (cur.getChar() == 'i') {
                                value.append("i");
                                cur.nextCp();
                                if (cur.getChar() == 'n') {
                                    value.append("n");
                                    cur.nextCp();
                                    if (cur.getChar() == 'g') {
                                        value.append("g");
                                        cur.nextCp();
                                        if (cur.isWhiteSpace() || cur.isSpecial()) {
                                            return new KeywordToken(start, cur.copy(), TokenTag.HAVING);
                                        } else {
                                            return getIdent(start, value);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    return getIdent(start, value);

                case 'i':
                    value = new StringBuilder("i");
                    cur.nextCp();
                    if (cur.getChar() == 'n') {
                        value.append("n");
                        cur.nextCp();
                        if (cur.getChar() == 'n') {
                            value.append("n");
                            cur.nextCp();
                            if (cur.getChar() == 'e') {
                                value.append("e");
                                cur.nextCp();
                                if (cur.getChar() == 'r') {
                                    value.append("r");
                                    cur.nextCp();
                                    if (cur.isWhiteSpace() || cur.isSpecial()) {
                                        return new KeywordToken(start, cur.copy(), TokenTag.INNER);
                                    } else {
                                        return getIdent(start, value);
                                    }
                                }
                            }
                        } else if (cur.getChar() == 't') {
                            value.append("t");
                            cur.nextCp();
                            if (cur.getChar() == 'e') {
                                value.append("e");
                                cur.nextCp();
                                if (cur.getChar() == 'r') {
                                    value.append("r");
                                    cur.nextCp();
                                    if (cur.getChar() == 's') {
                                        value.append("s");
                                        cur.nextCp();
                                        if (cur.getChar() == 'e') {
                                            value.append("e");
                                            cur.nextCp();
                                            if (cur.getChar() == 'c') {
                                                value.append("c");
                                                cur.nextCp();
                                                if (cur.getChar() == 't') {
                                                    value.append("t");
                                                    cur.nextCp();
                                                    if (cur.isWhiteSpace() || cur.isSpecial()) {
                                                        return new KeywordToken(start, cur.copy(), TokenTag.INTERSECT);
                                                    } else {
                                                        return getIdent(start, value);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        } else if (cur.isWhiteSpace() || cur.isSpecial()) {
                            return new KeywordToken(start, cur.copy(), TokenTag.IN);
                        } else {
                            return getIdent(start, value);
                        }
                    } else if (cur.getChar() == 's') {
                        value.append("s");
                        cur.nextCp();
                        if (cur.isWhiteSpace() || cur.isSpecial()) {
                            return new KeywordToken(start, cur.copy(), TokenTag.IS);
                        } else {
                            return getIdent(start, value);
                        }
                    }

                    return getIdent(start, value);

                case 'j':
                    value = new StringBuilder("j");
                    cur.nextCp();
                    if (cur.getChar() == 'o') {
                        value.append("o");
                        cur.nextCp();
                        if (cur.getChar() == 'i') {
                            value.append("i");
                            cur.nextCp();
                            if (cur.getChar() == 'n') {
                                value.append("n");
                                cur.nextCp();
                                if (cur.isWhiteSpace() || cur.isSpecial()) {
                                    return new KeywordToken(start, cur.copy(), TokenTag.JOIN);
                                } else {
                                    return getIdent(start, value);
                                }
                            }
                        }
                    }

                    return getIdent(start, value);

                case 'l':
                    value = new StringBuilder("l");
                    cur.nextCp();
                    if (cur.getChar() == 'e') {
                        value.append("e");
                        cur.nextCp();
                        if (cur.getChar() == 'f') {
                            value.append("f");
                            cur.nextCp();
                            if (cur.getChar() == 't') {
                                value.append("t");
                                cur.nextCp();
                                if (cur.isWhiteSpace() || cur.isSpecial()) {
                                    return new KeywordToken(start, cur.copy(), TokenTag.LEFT);
                                } else {
                                    return getIdent(start, value);
                                }
                            }
                        }
                    } else if (cur.getChar() == 'i') {
                        value.append("i");
                        cur.nextCp();
                        if (cur.getChar() == 'k') {
                            value.append("k");
                            cur.nextCp();
                            if (cur.getChar() == 'e') {
                                value.append("e");
                                cur.nextCp();
                                if (cur.isWhiteSpace() || cur.isSpecial()) {
                                    return new KeywordToken(start, cur.copy(), TokenTag.LIKE);
                                } else {
                                    return getIdent(start, value);
                                }
                            }
                        } else if (cur.getChar() == 'm') {
                            value.append("m");
                            cur.nextCp();
                            if (cur.getChar() == 'i') {
                                value.append("i");
                                cur.nextCp();
                                if (cur.getChar() == 't') {
                                    value.append("t");
                                    cur.nextCp();
                                    if (cur.isWhiteSpace() || cur.isSpecial()) {
                                        return new KeywordToken(start, cur.copy(), TokenTag.LIMIT);
                                    } else {
                                        return getIdent(start, value);
                                    }
                                }
                            }
                        }
                    }

                    return getIdent(start, value);

                case 'm':
                    value = new StringBuilder("m");
                    cur.nextCp();
                    if (cur.getChar() == 'a') {
                        value.append("a");
                        cur.nextCp();
                        if (cur.getChar() == 'x') {
                            value.append("x");
                            cur.nextCp();
                            if (cur.isWhiteSpace() || cur.isSpecial()) {
                                return new KeywordToken(start, cur.copy(), TokenTag.MAX);
                            } else {
                                return getIdent(start, value);
                            }
                        }
                    } else if (cur.getChar() == 'i') {
                        value.append("i");
                        cur.nextCp();
                        if (cur.getChar() == 'n') {
                            value.append("n");
                            cur.nextCp();
                            if (cur.isWhiteSpace() || cur.isSpecial()) {
                                return new KeywordToken(start, cur.copy(), TokenTag.MIN);
                            } else {
                                return getIdent(start, value);
                            }
                        }
                    }

                    return getIdent(start, value);

                case 'n':
                    value = new StringBuilder("n");
                    cur.nextCp();
                    if (cur.getChar() == 'o') {
                        value.append('o');
                        cur.nextCp();
                        if (cur.getChar() == 't') {
                            value.append("t");
                            cur.nextCp();
                            if (cur.isWhiteSpace() || cur.isSpecial()) {
                                return new KeywordToken(start, cur.copy(), TokenTag.NOT);
                            } else {
                                return getIdent(start, value);
                            }
                        }
                    } else if (cur.getChar() == 'u') {
                        value.append('u');
                        cur.nextCp();
                        if (cur.getChar() == 'l') {
                            value.append("l");
                            cur.nextCp();
                            if (cur.getChar() == 'l') {
                                value.append("l");
                                cur.nextCp();
                                if (cur.isWhiteSpace() || cur.isSpecial()) {
                                    return new BoolToken(start, cur.copy(), null, TokenTag.NULL);
                                } else {
                                    return getIdent(start, value);
                                }
                            }
                        }
                    }

                    return getIdent(start, value);

                case 'o':
                    value = new StringBuilder("o");
                    cur.nextCp();
                    if (cur.getChar() == 'f') {
                        value.append("f");
                        cur.nextCp();
                        if (cur.getChar() == 'f') {
                            value.append("f");
                            cur.nextCp();
                            if (cur.getChar() == 's') {
                                value.append("s");
                                cur.nextCp();
                                if (cur.getChar() == 'e') {
                                    value.append("e");
                                    cur.nextCp();
                                    if (cur.getChar() == 't') {
                                        value.append("t");
                                        cur.nextCp();
                                        if (cur.isWhiteSpace() || cur.isSpecial()) {
                                            return new KeywordToken(start, cur.copy(), TokenTag.OFFSET);
                                        } else {
                                            return getIdent(start, value);
                                        }
                                    }
                                }
                            }
                        }
                    } else if (cur.getChar() == 'n') {
                        value.append("n");
                        cur.nextCp();
                        if (cur.isWhiteSpace() || cur.isSpecial()) {
                            return new KeywordToken(start, cur.copy(), TokenTag.ON);
                        } else {
                            return getIdent(start, value);
                        }
                    } else if (cur.getChar() == 'r') {
                        value.append("r");
                        cur.nextCp();
                        if (cur.isWhiteSpace() || cur.isSpecial()) {
                            return new KeywordToken(start, cur.copy(), TokenTag.OR);
                        } else {
                            return getIdent(start, value);
                        }
                    } else if (cur.getChar() == 'u') {
                        value.append("u");
                        cur.nextCp();
                        if (cur.getChar() == 't') {
                            value.append("t");
                            cur.nextCp();
                            if (cur.getChar() == 'e') {
                                value.append("e");
                                cur.nextCp();
                                if (cur.getChar() == 'r') {
                                    value.append("r");
                                    cur.nextCp();
                                    if (cur.isWhiteSpace() || cur.isSpecial()) {
                                        return new KeywordToken(start, cur.copy(), TokenTag.OUTER);
                                    } else {
                                        return getIdent(start, value);
                                    }
                                }
                            }
                        }
                    }

                    return getIdent(start, value);

                case 'r':
                    value = new StringBuilder("r");
                    cur.nextCp();
                    if (cur.getChar() == 'i') {
                        value.append("i");
                        cur.nextCp();
                        if (cur.getChar() == 'g') {
                            value.append("g");
                            cur.nextCp();
                            if (cur.getChar() == 'h') {
                                value.append("h");
                                cur.nextCp();
                                if (cur.getChar() == 't') {
                                    value.append("t");
                                    cur.nextCp();
                                    if (cur.isWhiteSpace() || cur.isSpecial()) {
                                        return new KeywordToken(start, cur.copy(), TokenTag.RIGHT);
                                    } else {
                                        return getIdent(start, value);
                                    }
                                }
                            }
                        }
                    }

                    return getIdent(start, value);

                case 's':
                    value = new StringBuilder("s");
                    cur.nextCp();
                    if (cur.getChar() == 'e') {
                        value.append("e");
                        cur.nextCp();
                        if (cur.getChar() == 'l') {
                            value.append("l");
                            cur.nextCp();
                            if (cur.getChar() == 'e') {
                                value.append("e");
                                cur.nextCp();
                                if (cur.getChar() == 'c') {
                                    value.append("c");
                                    cur.nextCp();
                                    if (cur.getChar() == 't') {
                                        value.append("t");
                                        cur.nextCp();
                                        if (cur.isWhiteSpace() || cur.isSpecial()) {
                                            return new KeywordToken(start, cur.copy(), TokenTag.SELECT);
                                        } else {
                                            return getIdent(start, value);
                                        }
                                    }
                                }
                            }
                        }
                    } else if (cur.getChar() == 'u') {
                        value.append("u");
                        cur.nextCp();
                        if (cur.getChar() == 'm') {
                            value.append("m");
                            cur.nextCp();
                            if (cur.isWhiteSpace() || cur.isSpecial()) {
                                return new KeywordToken(start, cur.copy(), TokenTag.SUM);
                            } else {
                                return getIdent(start, value);
                            }
                        }
                    }

                    return getIdent(start, value);

                case 't':
                    value = new StringBuilder("t");
                    cur.nextCp();
                    if (cur.getChar() == 'r') {
                        value.append("r");
                        cur.nextCp();
                        if (cur.getChar() == 'u') {
                            value.append("u");
                            cur.nextCp();
                            if (cur.getChar() == 'e') {
                                value.append("e");
                                cur.nextCp();
                                if (cur.isWhiteSpace() || cur.isSpecial()) {
                                    return new BoolToken(start, cur.copy(), true, TokenTag.TRUE);
                                } else {
                                    return getIdent(start, value);
                                }
                            }
                        }
                    }

                    return getIdent(start, value);

                case 'u':
                    value = new StringBuilder("u");
                    cur.nextCp();
                    if (cur.getChar() == 'n') {
                        value.append("n");
                        cur.nextCp();
                        if (cur.getChar() == 'i') {
                            value.append("i");
                            cur.nextCp();
                            if (cur.getChar() == 'o') {
                                value.append("o");
                                cur.nextCp();
                                if (cur.getChar() == 'n') {
                                    value.append("n");
                                    cur.nextCp();
                                    if (cur.isWhiteSpace() || cur.isSpecial()) {
                                        return new KeywordToken(start, cur.copy(), TokenTag.UNION);
                                    } else {
                                        return getIdent(start, value);
                                    }
                                }
                            }
                        }
                    } else if (cur.getChar() == 's') {
                        value.append("s");
                        cur.nextCp();
                        if (cur.getChar() == 'i') {
                            value.append("i");
                            cur.nextCp();
                            if (cur.getChar() == 'n') {
                                value.append("n");
                                cur.nextCp();
                                if (cur.getChar() == 'g') {
                                    value.append("g");
                                    cur.nextCp();
                                    if (cur.isWhiteSpace() || cur.isSpecial()) {
                                        return new KeywordToken(start, cur.copy(), TokenTag.USING);
                                    } else {
                                        return getIdent(start, value);
                                    }
                                }
                            }
                        }
                    }

                    return getIdent(start, value);

                case 'w':
                    value = new StringBuilder("w");
                    cur.nextCp();

                    if (cur.getChar() == 'h') {
                        value.append("h");
                        cur.nextCp();
                        if (cur.getChar() == 'e') {
                            value.append("e");
                            cur.nextCp();
                            if (cur.getChar() == 'r') {
                                value.append("r");
                                cur.nextCp();
                                if (cur.getChar() == 'e') {
                                    value.append("e");
                                    cur.nextCp();
                                    if (cur.isWhiteSpace() || cur.isSpecial()) {
                                        return new KeywordToken(start, cur.copy(), TokenTag.WHERE);
                                    } else {
                                        return getIdent(start, value);
                                    }
                                }
                            }
                        }
                    }

                case '(':
                    cur.nextCp();

                    return new SpecToken(TokenTag.LPAREN, start, cur.copy(), "(");
                case ')':
                    cur.nextCp();

                    return new SpecToken(TokenTag.RPAREN, start, cur.copy(), ")");

                case '{':
                    cur.nextCp();

                    return new SpecToken(TokenTag.LBRACE, start, cur.copy(), "{");
                case '}':
                    cur.nextCp();

                    return new SpecToken(TokenTag.RBRACE, start, cur.copy(), "}");
                case '<':
                    cur.nextCp();
                    if (cur.getChar() == '=') {
                        cur.nextCp();

                        return new SpecToken(TokenTag.LESSEQ, start, cur.copy(), "<=");
                    } else

                        return new SpecToken(TokenTag.LESS, start, cur.copy(), "<");
                case '>':
                    cur.nextCp();
                    if (cur.getChar() == '=') {
                        cur.nextCp();

                        return new SpecToken(TokenTag.GREATEREQ, start, cur.copy(), ">=");
                    } else

                        return new SpecToken(TokenTag.GREATER, start, cur.copy(), ">");
                case '=':
                    cur.nextCp();

                    return new SpecToken(TokenTag.EQUAL, start, cur.copy(), "=");
                case '!':
                    cur.nextCp();
                    if (cur.getChar() == '=') {
                        cur.nextCp();
                    } else {
                        error("'=' expected");
                    }

                    return new SpecToken(TokenTag.NOTEQUAL, start, cur.copy(), "!=");
                case '+':
                    cur.nextCp();

                    return new SpecToken(TokenTag.ADD, start, cur.copy(), "+");
                case '-':
                    cur.nextCp();
                    if (cur.getChar() == '-') {
                        while (cur.getChar() != '\n' && cur.getChar() != '\r')
                            cur.nextCp();

                        continue;
                    }

                    return new SpecToken(TokenTag.SUB, start, cur.copy(), "-");
                case '*':
                    cur.nextCp();

                    return new SpecToken(TokenTag.MUL, start, cur.copy(), "*");
                case '/':
                    cur.nextCp();

                    return new SpecToken(TokenTag.DIV, start, cur.copy(), "/");
                case '\'':
                    value = new StringBuilder();
                    cur.nextCp();
                    while (cur.getChar() != '\'' && cur.getChar() != (char) 0xFFFFFFFF) {
                        if (cur.getChar() == '\n' || cur.getChar() == '\r')
                            error("String must be in one line");
                        else
                            value.append(cur.getChar());

                        cur.nextCp();
                    }

                    cur.nextCp();

//                    try {
//                        Timestamp date = Timestamp.valueOf(LocalDateTime.parse(value.toString(), DateTimeFormatter.ISO_DATE_TIME));
//                        return new DateTimeToken(TokenTag.TIMESTAMP_CONST, start, cur.copy(), date.toLocalDateTime());
//                    } catch (DateTimeParseException e1) {
//                        try {
//                            Date date = Date.valueOf(LocalDate.parse(value.toString(), DateTimeFormatter.ISO_DATE));
//                            return new DateTimeToken(TokenTag.DATE_CONST, start, cur.copy(), date.toLocalDate());
//                        } catch (DateTimeParseException e2) {
//                            try {
//                                Time date = Time.valueOf(LocalTime.parse(value.toString(), DateTimeFormatter.ISO_TIME));
//                                return new DateTimeToken(TokenTag.TIME_CONST, start, cur.copy(), date.toLocalTime());
//                            } catch (DateTimeParseException e3) {
//                                return new StringToken(start, cur.copy(), value.toString());
//                            }
//                        }
//                    }
                case '"':
                    value = new StringBuilder().append('"');
                    cur.nextCp();
                    while (cur.getChar() != '"' && cur.getChar() != (char) 0xFFFFFFFF) {
                        if (cur.getChar() == '\n' || cur.getChar() == '\r')
                            error("Identifier can't contain new line symbols");
                        else
                            value.append(cur.getChar());

                        cur.nextCp();
                    }
                    value.append('"');
                    cur.nextCp();

                    return new IdentToken(start, cur.copy(), value.toString());
                case ',':
                    cur.nextCp();

                    return new SpecToken(TokenTag.COMMA, start, cur.copy(), ",");
                case '.':
                    cur.nextCp();

                    return new SpecToken(TokenTag.DOT, start, cur.copy(), ".");

                default:
                   if (cur.isLetter()) {
                       return getIdent(start, new StringBuilder());
                   } else if (cur.isDigit()) {
                       Token number = getNumber(start);
                       if (number != null) {
                           return number;
                       } else {
                           error("Unrecognizable number");
                           cur.nextCp();
                           break;
                       }
                   } else {
                       error("Unrecognizable token");
                       cur.nextCp();
                       break;
                   }
            }
        }

        return new EOFToken(cur);
    }

    private IdentToken getIdent(Position start, StringBuilder value) {
        while (cur.isLetterOrDigit() || cur.getChar() == '_') {
            value.append(cur.getChar());
            cur.nextCp();
        }

        return new IdentToken(start, cur.copy(), value.toString());
    }

    private NumberToken getNumber(Position start) {
        StringBuilder value = new StringBuilder();
        boolean wasComma = false;
        while (cur.isDigit() || cur.getChar() == '.') {
            if (cur.getChar() == '.') {
                if (wasComma)
                    error("Two dots in float number");
                else
                    wasComma = true;
            }

            value.append(cur.getChar());
            cur.nextCp();
        }

        if (wasComma) {
            try {
                Float number = Float.parseFloat(value.toString());
                return new NumberToken(TokenTag.FLOAT_CONST, start, cur.copy(), number);
            } catch (NumberFormatException ef) {
                try {
                    Double number = Double.parseDouble(value.toString());
                    return new NumberToken(TokenTag.DOUBLE_CONST, start, cur.copy(), number);
                } catch (NumberFormatException ed) {
                    error("Wrong number");
                    return null;
                }
            }
        } else {
            try {
                Byte number = Byte.parseByte(value.toString());
                return new NumberToken(TokenTag.BYTE_CONST, start, cur.copy(), number);
            } catch (NumberFormatException eb) {
                try {
                    Short number = Short.parseShort(value.toString());
                    return new NumberToken(TokenTag.SHORT_CONST, start, cur.copy(), number);
                } catch (NumberFormatException es) {
                    try {
                        Integer number = Integer.parseInt(value.toString());
                        return new NumberToken(TokenTag.INT_CONST, start, cur.copy(), number);
                    } catch (NumberFormatException ei) {
                        try {
                            Long number = Long.parseLong(value.toString());
                            return new NumberToken(TokenTag.LONG_CONST, start, cur.copy(), number);
                        } catch (NumberFormatException el) {
                            error("Wrong number");
                            return null;
                        }
                    }
                }
            }
        }
    }

    private void error(String msg) {
        messages.add(new Message(cur.copy(), msg));
        logger.warn(msg);
    }

    public List<Message> getMessages() {
        return messages;
    }
}
