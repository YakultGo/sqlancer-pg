package sqlancer.postgres.ast;

import java.util.Arrays;

import sqlancer.IgnoreMeException;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.ast.PostgresTemporalUtil.TemporalField;

public final class PostgresTemporalFunction implements PostgresExpression {

    public enum TemporalFunctionKind {
        DATE_PART("date_part"), EXTRACT("extract"), MAKE_INTERVAL("make_interval"), JUSTIFY_HOURS("justify_hours"),
        JUSTIFY_DAYS("justify_days"), JUSTIFY_INTERVAL("justify_interval"), TIMEZONE("timezone"),
        DATE_TRUNC("date_trunc");

        private final String functionName;

        TemporalFunctionKind(String functionName) {
            this.functionName = functionName;
        }

        public String getFunctionName() {
            return functionName;
        }
    }

    private final TemporalFunctionKind kind;
    private final PostgresDataType returnType;
    private final String modifier;
    private final PostgresExpression[] args;
    private final boolean supportsExpectedValue;

    public PostgresTemporalFunction(TemporalFunctionKind kind, PostgresDataType returnType, String modifier,
            boolean supportsExpectedValue, PostgresExpression... args) {
        this.kind = kind;
        this.returnType = returnType;
        this.modifier = modifier;
        this.args = args.clone();
        this.supportsExpectedValue = supportsExpectedValue;
    }

    public TemporalFunctionKind getKind() {
        return kind;
    }

    public String getModifier() {
        return modifier;
    }

    public PostgresExpression[] getArguments() {
        return Arrays.copyOf(args, args.length);
    }

    @Override
    public PostgresConstant getExpectedValue() {
        if (!supportsExpectedValue) {
            return null;
        }
        PostgresConstant[] constants = new PostgresConstant[args.length];
        for (int i = 0; i < args.length; i++) {
            constants[i] = args[i].getExpectedValue();
            if (constants[i] == null) {
                return null;
            }
            if (constants[i].isNull()) {
                return PostgresConstant.createNullConstant();
            }
        }
        switch (kind) {
        case DATE_PART:
        case EXTRACT:
            return PostgresConstant.createIntConstant(PostgresTemporalUtil.extractField(TemporalField.fromString(modifier),
                    args[0].getExpressionType(), constants[0].getUnquotedTextRepresentation()));
        case MAKE_INTERVAL:
            return PostgresConstant.createIntervalConstant(PostgresTemporalUtil.makeInterval(asInt(constants[0]),
                    asInt(constants[1]), asInt(constants[2]), asInt(constants[3]), asInt(constants[4]),
                    asInt(constants[5]), asInt(constants[6])));
        case JUSTIFY_HOURS:
            return PostgresConstant
                    .createIntervalConstant(PostgresTemporalUtil.justifyHours(constants[0].getUnquotedTextRepresentation()));
        case JUSTIFY_DAYS:
            return PostgresConstant
                    .createIntervalConstant(PostgresTemporalUtil.justifyDays(constants[0].getUnquotedTextRepresentation()));
        case JUSTIFY_INTERVAL:
            return PostgresConstant.createIntervalConstant(
                    PostgresTemporalUtil.justifyInterval(constants[0].getUnquotedTextRepresentation()));
        case TIMEZONE:
            return PostgresConstant.createTimestampConstant(
                    PostgresTemporalUtil.timezone(modifier, constants[0].getUnquotedTextRepresentation()));
        case DATE_TRUNC:
            return createTemporalResult(PostgresTemporalUtil.dateTrunc(TemporalField.fromString(modifier),
                    args[0].getExpressionType(), constants[0].getUnquotedTextRepresentation()));
        default:
            throw new IgnoreMeException();
        }
    }

    private static int asInt(PostgresConstant constant) {
        return (int) constant.cast(PostgresDataType.INT).asInt();
    }

    private PostgresConstant createTemporalResult(String value) {
        switch (returnType) {
        case TIMESTAMP:
            return PostgresConstant.createTimestampConstant(value);
        case TIMESTAMPTZ:
            return PostgresConstant.createTimestampWithTimeZoneConstant(value);
        case INTERVAL:
            return PostgresConstant.createIntervalConstant(value);
        default:
            throw new IgnoreMeException();
        }
    }

    @Override
    public PostgresDataType getExpressionType() {
        return returnType;
    }
}
