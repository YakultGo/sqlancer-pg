package sqlancer.postgres.ast;

import sqlancer.postgres.PostgresCompoundDataType;
import sqlancer.common.ast.newast.Expression;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public interface PostgresExpression extends Expression<PostgresColumn> {

    default PostgresDataType getExpressionType() {
        return null;
    }

    default PostgresConstant getExpectedValue() {
        return null;
    }

    default PostgresCompoundDataType getExpressionCompoundType() {
        PostgresDataType type = getExpressionType();
        if (type == null) {
            return null;
        }
        return PostgresCompoundDataType.create(type);
    }
}
