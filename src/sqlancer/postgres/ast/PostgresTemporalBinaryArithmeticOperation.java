package sqlancer.postgres.ast;

import sqlancer.IgnoreMeException;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public final class PostgresTemporalBinaryArithmeticOperation
        extends BinaryOperatorNode<PostgresExpression, PostgresTemporalBinaryArithmeticOperation.TemporalBinaryOperator>
        implements PostgresExpression {

    public enum TemporalBinaryOperator implements Operator {
        ADDITION("+"), SUBTRACTION("-");

        private final String textRepresentation;

        TemporalBinaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

    private final PostgresDataType returnType;

    public PostgresTemporalBinaryArithmeticOperation(PostgresExpression left, PostgresExpression right,
            TemporalBinaryOperator op, PostgresDataType returnType) {
        super(left, right, op);
        this.returnType = returnType;
    }

    @Override
    public PostgresConstant getExpectedValue() {
        PostgresConstant left = getLeft().getExpectedValue();
        PostgresConstant right = getRight().getExpectedValue();
        if (left == null || right == null) {
            return null;
        }
        if (left.isNull() || right.isNull()) {
            return PostgresConstant.createNullConstant();
        }
        String leftText = left.getUnquotedTextRepresentation();
        String rightText = right.getUnquotedTextRepresentation();
        TemporalBinaryOperator op = getOp();
        switch (returnType) {
        case TIME:
            return PostgresConstant
                    .createTimeConstant(applyTemporal(op, getLeft().getExpressionType(), leftText, rightText));
        case TIMETZ:
            return PostgresConstant
                    .createTimeWithTimeZoneConstant(applyTemporal(op, getLeft().getExpressionType(), leftText, rightText));
        case TIMESTAMP:
            return PostgresConstant
                    .createTimestampConstant(applyTemporal(op, getLeft().getExpressionType(), leftText, rightText));
        case TIMESTAMPTZ:
            return PostgresConstant.createTimestampWithTimeZoneConstant(
                    applyTemporal(op, getLeft().getExpressionType(), leftText, rightText));
        case INTERVAL:
            if (getLeft().getExpressionType() == PostgresDataType.INTERVAL
                    && getRight().getExpressionType() == PostgresDataType.INTERVAL) {
                return PostgresConstant.createIntervalConstant(op == TemporalBinaryOperator.ADDITION
                        ? PostgresTemporalUtil.addIntervals(leftText, rightText)
                        : PostgresTemporalUtil.subtractIntervals(leftText, rightText));
            }
            PostgresDataType leftType = getLeft().getExpressionType();
            if (leftType == PostgresDataType.TIMESTAMP || leftType == PostgresDataType.TIMESTAMPTZ) {
                if (op != TemporalBinaryOperator.SUBTRACTION) {
                    throw new IgnoreMeException();
                }
                return PostgresConstant.createIntervalConstant(
                        PostgresTemporalUtil.subtractTimestamps(leftType, leftText, rightText));
            }
            throw new IgnoreMeException();
        case INT:
            if (getLeft().getExpressionType() == PostgresDataType.DATE
                    && getRight().getExpressionType() == PostgresDataType.DATE
                    && op == TemporalBinaryOperator.SUBTRACTION) {
                return PostgresConstant.createIntConstant(PostgresTemporalUtil.subtractDates(leftText, rightText));
            }
            throw new IgnoreMeException();
        default:
            throw new IgnoreMeException();
        }
    }

    private String applyTemporal(TemporalBinaryOperator op, PostgresDataType type, String left, String right) {
        if (op == TemporalBinaryOperator.ADDITION) {
            return PostgresTemporalUtil.addInterval(type, left, right);
        }
        return PostgresTemporalUtil.subtractInterval(type, left, right);
    }

    @Override
    public PostgresDataType getExpressionType() {
        return returnType;
    }
}
