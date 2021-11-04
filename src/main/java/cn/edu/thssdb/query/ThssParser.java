package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.query.base.Statement;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.util.ArrayList;

public class ThssParser {
    public ThssVisitor visitor;
    public ThssParser() {
        this.visitor = new ThssVisitor();
    }

    public ArrayList<Statement> parseThss_stmts(String statement) {
        SQLLexer ThssLexer = new SQLLexer(CharStreams.fromString(statement));
        SQLParser ThssParser = new SQLParser(new CommonTokenStream(ThssLexer));
        try {
            ArrayList<Statement> stmts = (ArrayList<Statement>) visitor.visit(ThssParser.parse());
            return stmts;
        } catch (Exception e) {
            throw e;
        }
    }
}
