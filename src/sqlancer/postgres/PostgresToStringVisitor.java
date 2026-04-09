package sqlancer.postgres;

import java.util.List;
import java.util.Optional;

import sqlancer.Randomly;
import sqlancer.common.visitor.BinaryOperation;
import sqlancer.common.visitor.ToStringVisitor;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.ast.PostgresAggregate;
import sqlancer.postgres.ast.PostgresBetweenOperation;
import sqlancer.postgres.ast.PostgresBinaryLogicalOperation;
import sqlancer.postgres.ast.PostgresCastOperation;
import sqlancer.postgres.ast.PostgresCollate;
import sqlancer.postgres.ast.PostgresColumnReference;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresConstant;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresFunction;
import sqlancer.postgres.ast.PostgresInOperation;
import sqlancer.postgres.ast.PostgresJoin;
import sqlancer.postgres.ast.PostgresJoin.PostgresJoinType;
import sqlancer.postgres.ast.PostgresLikeOperation;
import sqlancer.postgres.ast.PostgresOrderByTerm;
import sqlancer.postgres.ast.PostgresPOSIXRegularExpression;
import sqlancer.postgres.ast.PostgresPostfixOperation;
import sqlancer.postgres.ast.PostgresPostfixText;
import sqlancer.postgres.ast.PostgresPrefixOperation;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.ast.PostgresSelect.PostgresSubquery;
import sqlancer.postgres.ast.PostgresSimilarTo;
import sqlancer.postgres.ast.PostgresTableReference;
import sqlancer.postgres.ast.PostgresTemporalFunction;
import sqlancer.postgres.ast.PostgresTemporalFunction.TemporalFunctionKind;
import sqlancer.postgres.ast.PostgresWindowFunction;
import sqlancer.postgres.ast.PostgresWindowFunction.WindowFrame;
import sqlancer.postgres.ast.PostgresWindowFunction.WindowSpecification;

public final class PostgresToStringVisitor extends ToStringVisitor<PostgresExpression> implements PostgresVisitor {

    @Override
    public void visitSpecific(PostgresExpression expr) {
        PostgresVisitor.super.visit(expr);
    }

    @Override
    public void visit(PostgresConstant constant) {
        sb.append(constant.getTextRepresentation());
    }

    @Override
    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(PostgresColumnReference column) {
        sb.append(column.getColumn().getFullQualifiedName());
    }

    @Override
    public void visit(PostgresPostfixOperation op) {
        sb.append("(");
        visit(op.getExpression());
        sb.append(")");
        sb.append(" ");
        sb.append(op.getOperatorTextRepresentation());
    }

    @Override
    public void visit(PostgresColumnValue c) {
        sb.append(c.getColumn().getFullQualifiedName());
    }

    @Override
    public void visit(PostgresPrefixOperation op) {
        sb.append(op.getTextRepresentation());
        sb.append(" (");
        visit(op.getExpression());
        sb.append(")");
    }

    @Override
    public void visit(PostgresFromTable from) {
        if (from.isOnly()) {
            sb.append("ONLY ");
        }
        sb.append(from.getTable().getName());
        if (!from.isOnly() && Randomly.getBoolean()) {
            sb.append("*");
        }
    }

    @Override
    public void visit(PostgresSubquery subquery) {
        sb.append("(");
        visit(subquery.getSelect());
        sb.append(") AS ");
        sb.append(subquery.getName());
    }

    @Override
    public void visit(PostgresTableReference ref) {
        sb.append(ref.getTable().getName());
    }

    @Override
    public void visit(PostgresSelect s) {
        sb.append("SELECT ");
        switch (s.getSelectOption()) {
        case DISTINCT:
            sb.append("DISTINCT ");
            if (s.getDistinctOnClause() != null) {
                sb.append("ON (");
                visit(s.getDistinctOnClause());
                sb.append(") ");
            }
            break;
        case ALL:
            sb.append(Randomly.fromOptions("ALL ", ""));
            break;
        default:
            throw new AssertionError();
        }
        if (s.getFetchColumns() == null) {
            sb.append("*");
        } else {
            visit(s.getFetchColumns());
        }
        sb.append(" FROM ");
        visit(s.getFromList());

        for (PostgresJoin j : s.getJoinClauses()) {
            sb.append(" ");
            switch (j.getType()) {
            case INNER:
                if (Randomly.getBoolean()) {
                    sb.append("INNER ");
                }
                sb.append("JOIN");
                break;
            case LEFT:
                sb.append("LEFT OUTER JOIN");
                break;
            case RIGHT:
                sb.append("RIGHT OUTER JOIN");
                break;
            case FULL:
                sb.append("FULL OUTER JOIN");
                break;
            case CROSS:
                sb.append("CROSS JOIN");
                break;
            default:
                throw new AssertionError(j.getType());
            }
            sb.append(" ");
            visit(j.getTableReference());
            if (j.getType() != PostgresJoinType.CROSS) {
                sb.append(" ON ");
                visit(j.getOnClause());
            }
        }

        if (s.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(s.getWhereClause());
        }
        if (!s.getGroupByExpressions().isEmpty()) {
            sb.append(" GROUP BY ");
            visit(s.getGroupByExpressions());
        }
        if (s.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(s.getHavingClause());

        }
        if (!s.getOrderByClauses().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(s.getOrderByClauses());
        }
        if (s.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(s.getLimitClause());
        }

        if (s.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(s.getOffsetClause());
        }
        if (canRenderForClause(s)) {
            sb.append(" FOR ");
            sb.append(s.getForClause().getTextRepresentation());
            if (!s.getForClauseOfReferences().isEmpty()) {
                sb.append(" OF ");
                sb.append(String.join(", ", s.getForClauseOfReferences()));
            }
            if (s.getLockWaitOption() != null && !s.getLockWaitOption().getTextRepresentation().isEmpty()) {
                sb.append(" ");
                sb.append(s.getLockWaitOption().getTextRepresentation());
            }
        }
    }

    private boolean canRenderForClause(PostgresSelect select) {
        if (!select.isAllowForClause() || select.getForClause() == null) {
            return false;
        }
        if (select.getSelectOption() != PostgresSelect.SelectType.ALL || select.getDistinctOnClause() != null) {
            return false;
        }
        if (!select.getGroupByExpressions().isEmpty() || select.getHavingClause() != null) {
            return false;
        }
        if (select.getWindowFunctions() != null && !select.getWindowFunctions().isEmpty()) {
            return false;
        }
        if (select.getFetchColumns() != null
                && select.getFetchColumns().stream().anyMatch(PostgresAggregate.class::isInstance)) {
            return false;
        }
        if (select.getFromList().stream().anyMatch(this::locksDerivedOrViewRelation)) {
            return false;
        }
        if (select.getJoinClauses().stream().map(PostgresJoin::getTableReference).anyMatch(this::locksDerivedOrViewRelation)) {
            return false;
        }
        return select.getJoinClauses().stream().noneMatch(j -> j.getType() == PostgresJoinType.LEFT
                || j.getType() == PostgresJoinType.RIGHT || j.getType() == PostgresJoinType.FULL);
    }

    private boolean locksDerivedOrViewRelation(PostgresExpression relation) {
        if (relation instanceof PostgresSubquery) {
            return true;
        }
        if (relation instanceof PostgresFromTable) {
            return ((PostgresFromTable) relation).getTable().isView();
        }
        return false;
    }

    @Override
    public void visit(PostgresOrderByTerm term) {
        visit(term.getExpr());
        sb.append(term.isAscending() ? " ASC" : " DESC");
    }

    @Override
    public void visit(PostgresFunction f) {
        sb.append(f.getFunctionName());
        sb.append("(");
        int i = 0;
        for (PostgresExpression arg : f.getArguments()) {
            if (i++ != 0) {
                sb.append(", ");
            }
            visit(arg);
        }
        sb.append(")");
    }

    @Override
    public void visit(PostgresTemporalFunction function) {
        if (function.getKind() == TemporalFunctionKind.EXTRACT) {
            sb.append("EXTRACT(");
            sb.append(function.getModifier());
            sb.append(" FROM ");
            visit(function.getArguments()[0]);
            sb.append(")");
            return;
        }
        sb.append(function.getKind().getFunctionName());
        sb.append("(");
        if (function.getModifier() != null) {
            sb.append("'");
            sb.append(function.getModifier().replace("'", "''"));
            sb.append("'");
            if (function.getArguments().length != 0) {
                sb.append(", ");
            }
        }
        int i = 0;
        for (PostgresExpression arg : function.getArguments()) {
            if (i++ != 0) {
                sb.append(", ");
            }
            visit(arg);
        }
        sb.append(")");
    }

    @Override
    public void visit(PostgresCastOperation cast) {
        if (Randomly.getBoolean()) {
            sb.append("CAST(");
            visit(cast.getExpression());
            sb.append(" AS ");
            appendType(cast);
            sb.append(")");
        } else {
            sb.append("(");
            visit(cast.getExpression());
            sb.append(")::");
            appendType(cast);
        }
    }

    private void appendType(PostgresCastOperation cast) {
        PostgresCompoundDataType compoundType = cast.getCompoundType();
        switch (compoundType.getDataType()) {
        case BOOLEAN:
            sb.append("BOOLEAN");
            break;
        case INT: // TODO support also other int types
            sb.append("INT");
            break;
        case TEXT:
            // TODO: append TEXT, CHAR
            sb.append(Randomly.fromOptions("VARCHAR"));
            break;
        case REAL:
            sb.append("FLOAT");
            break;
        case DECIMAL:
            sb.append("DECIMAL");
            break;
        case FLOAT:
            sb.append("REAL");
            break;
        case RANGE:
            sb.append("int4range");
            break;
        case MONEY:
            sb.append("MONEY");
            break;
        case INET:
            sb.append("INET");
            break;
        case BIT:
            sb.append("BIT");
            // if (Randomly.getBoolean()) {
            // sb.append("(");
            // sb.append(Randomly.getNotCachedInteger(1, 100));
            // sb.append(")");
            // }
            break;
        case DATE:
            sb.append("DATE");
            break;
        case TIME:
            sb.append("TIME");
            break;
        case TIMETZ:
            sb.append("TIME WITH TIME ZONE");
            break;
        case TIMESTAMP:
            sb.append("TIMESTAMP");
            break;
        case TIMESTAMPTZ:
            sb.append("TIMESTAMP WITH TIME ZONE");
            break;
        case INTERVAL:
            sb.append("INTERVAL");
            break;
        default:
            throw new AssertionError(cast.getType());
        }
        Optional<Integer> size = compoundType.getSize();
        if (size.isPresent()) {
            sb.append("(");
            sb.append(size.get());
            sb.append(")");
        }
    }

    @Override
    public void visit(PostgresBetweenOperation op) {
        sb.append("(");
        visit(op.getExpr());
        if (PostgresProvider.generateOnlyKnown && op.getExpr().getExpressionType() == PostgresDataType.TEXT
                && op.getLeft().getExpressionType() == PostgresDataType.TEXT) {
            sb.append(" COLLATE \"C\"");
        }
        sb.append(") BETWEEN ");
        if (op.isSymmetric()) {
            sb.append("SYMMETRIC ");
        }
        sb.append("(");
        visit(op.getLeft());
        sb.append(") AND (");
        visit(op.getRight());
        if (PostgresProvider.generateOnlyKnown && op.getExpr().getExpressionType() == PostgresDataType.TEXT
                && op.getRight().getExpressionType() == PostgresDataType.TEXT) {
            sb.append(" COLLATE \"C\"");
        }
        sb.append(")");
    }

    @Override
    public void visit(PostgresInOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(")");
        if (!op.isTrue()) {
            sb.append(" NOT");
        }
        sb.append(" IN (");
        visit(op.getListElements());
        sb.append(")");
    }

    @Override
    public void visit(PostgresPostfixText op) {
        visit(op.getExpr());
        sb.append(op.getText());
    }

    @Override
    public void visit(PostgresAggregate op) {
        sb.append(op.getFunction());
        sb.append("(");
        visit(op.getArgs());
        sb.append(")");
    }

    @Override
    public void visit(PostgresSimilarTo op) {
        sb.append("(");
        visit(op.getString());
        sb.append(" SIMILAR TO ");
        visit(op.getSimilarTo());
        if (op.getEscapeCharacter() != null) {
            visit(op.getEscapeCharacter());
        }
        sb.append(")");
    }

    @Override
    public void visit(PostgresPOSIXRegularExpression op) {
        visit(op.getString());
        sb.append(op.getOp().getStringRepresentation());
        visit(op.getRegex());
    }

    @Override
    public void visit(PostgresCollate op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(" COLLATE ");
        sb.append('"');
        sb.append(op.getCollate());
        sb.append('"');
        sb.append(")");
    }

    @Override
    public void visit(PostgresBinaryLogicalOperation op) {
        super.visit((BinaryOperation<PostgresExpression>) op);
    }

    @Override
    public void visit(PostgresLikeOperation op) {
        super.visit((BinaryOperation<PostgresExpression>) op);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void visit(PostgresWindowFunction windowFunction) {
        sb.append(windowFunction.getFunctionName());
        sb.append("(");
        visit(windowFunction.getArguments());
        sb.append(") OVER (");

        WindowSpecification spec = windowFunction.getWindowSpec();
        if (!spec.getPartitionBy().isEmpty()) {
            sb.append("PARTITION BY ");
            visit(spec.getPartitionBy());
        }

        if (!spec.getOrderBy().isEmpty()) {
            if (!spec.getPartitionBy().isEmpty()) {
                sb.append(" ");
            }
            sb.append("ORDER BY ");
            visit((List<PostgresExpression>) (List<?>) spec.getOrderBy());
        }

        if (spec.getFrame() != null) {
            sb.append(" ");
            WindowFrame frame = spec.getFrame();
            sb.append(frame.getType().getSQL());
            sb.append(" BETWEEN ");
            visit(frame.getStartExpr());
            sb.append(" AND ");
            visit(frame.getEndExpr());
        }

        sb.append(")");
    }
}
