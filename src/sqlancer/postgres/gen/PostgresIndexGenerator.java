package sqlancer.postgres.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresCompoundDataType;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresIndex;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresCastOperation;
import sqlancer.postgres.ast.PostgresColumnReference;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresPostfixOperation;
import sqlancer.postgres.ast.PostgresPostfixOperation.PostfixOperator;
import sqlancer.postgres.ast.PostgresPostfixText;
import sqlancer.postgres.ast.PostgresPrefixOperation;
import sqlancer.postgres.ast.PostgresPrefixOperation.PrefixOperator;

public final class PostgresIndexGenerator {

    private PostgresIndexGenerator() {
    }

    public enum IndexType {
        BTREE, HASH, GIST, GIN
    }

    public enum PostgresIndexModel {
        DEFAULT(0),
        UNIQUE(1),
        PRIMARY_KEY(2),
        COMPOSITE(3),
        PREFIX_EXPR(4),
        SUFFIX_EXPR(5),
        EXPRESSION(6);

        private final int optionValue;

        PostgresIndexModel(int optionValue) {
            this.optionValue = optionValue;
        }

        public static PostgresIndexModel fromOption(int optionValue) {
            for (PostgresIndexModel model : values()) {
                if (model.optionValue == optionValue) {
                    return model;
                }
            }
            throw new AssertionError(optionValue);
        }

        public static PostgresIndexModel pickRandomNonDefault() {
            return Randomly.fromOptions(UNIQUE, PRIMARY_KEY, COMPOSITE, PREFIX_EXPR, SUFFIX_EXPR, EXPRESSION);
        }
    }

    private static final class IndexElement {
        private final String sql;

        private IndexElement(String sql) {
            this.sql = sql;
        }
    }

    public static SQLQueryAdapter generate(PostgresGlobalState globalState) {
        PostgresIndexModel configuredModel = PostgresIndexModel.fromOption(globalState.getOptions().getIndexModel());
        PostgresIndexModel effectiveModel = configuredModel == PostgresIndexModel.DEFAULT
                ? PostgresIndexModel.pickRandomNonDefault()
                : configuredModel;
        switch (effectiveModel) {
        case PRIMARY_KEY:
            return generatePrimaryKey(globalState);
        case UNIQUE:
        case COMPOSITE:
        case PREFIX_EXPR:
        case SUFFIX_EXPR:
        case EXPRESSION:
            return generateCreateIndex(globalState, effectiveModel);
        default:
            throw new AssertionError(effectiveModel);
        }
    }

    private static SQLQueryAdapter generatePrimaryKey(PostgresGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        PostgresTable randomTable = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<PostgresColumn> columns = getOrderedColumns(randomTable, Math.min(randomTable.getColumns().size(),
                Math.max(1, Randomly.smallNumber() + 1)), false, false);
        String constraintName = getNewIndexName(randomTable);
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ");
        sb.append(randomTable.getName());
        sb.append(" ADD CONSTRAINT ");
        sb.append(constraintName);
        sb.append(" PRIMARY KEY(");
        sb.append(columns.stream().map(PostgresColumn::getName).collect(Collectors.joining(", ")));
        sb.append(")");
        addPrimaryKeyErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static SQLQueryAdapter generateCreateIndex(PostgresGlobalState globalState, PostgresIndexModel model) {
        ExpectedErrors errors = new ExpectedErrors();
        PostgresTable randomTable = globalState.getSchema().getRandomTable(t -> !t.isView());
        boolean unique = model == PostgresIndexModel.UNIQUE;
        IndexType method = chooseIndexType(model, unique);
        List<IndexElement> elements = createIndexElements(globalState, randomTable, model, method, errors);
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE");
        if (unique) {
            sb.append(" UNIQUE");
        }
        sb.append(" INDEX ");
        sb.append(getNewIndexName(randomTable));
        sb.append(" ON ");
        if (Randomly.getBoolean()) {
            sb.append("ONLY ");
        }
        sb.append(randomTable.getName());
        if (method != IndexType.BTREE || Randomly.getBoolean()) {
            sb.append(" USING ");
            sb.append(method);
        }
        sb.append("(");
        sb.append(elements.stream().map(e -> e.sql).collect(Collectors.joining(", ")));
        sb.append(")");
        if (canUseInclude(method) && Randomly.getBoolean()) {
            sb.append(" INCLUDE(");
            List<PostgresColumn> columns = randomTable.getRandomNonEmptyColumnSubset();
            sb.append(columns.stream().map(PostgresColumn::getName).collect(Collectors.joining(", ")));
            sb.append(")");
        }
        if (canUseWhereClause(model) && Randomly.getBoolean()) {
            sb.append(" WHERE ");
            PostgresExpression expr = new PostgresExpressionGenerator(globalState).setColumns(randomTable.getColumns())
                    .setGlobalState(globalState).generateExpression(PostgresDataType.BOOLEAN);
            sb.append(PostgresVisitor.asString(expr));
        }
        addSharedIndexErrors(errors);
        if (unique) {
            addUniqueErrors(errors);
        }
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static List<IndexElement> createIndexElements(PostgresGlobalState globalState, PostgresTable randomTable,
            PostgresIndexModel model, IndexType method, ExpectedErrors errors) {
        if (method == IndexType.HASH) {
            return createHashIndexElements(randomTable);
        }
        int minColumns = model == PostgresIndexModel.COMPOSITE ? 2 : 1;
        int maxColumns = Math.max(minColumns, Math.min(randomTable.getColumns().size(), Randomly.smallNumber() + 1));
        boolean preferPrefixOrder = model == PostgresIndexModel.PREFIX_EXPR;
        boolean preferSuffixOrder = model == PostgresIndexModel.SUFFIX_EXPR;
        List<PostgresColumn> orderedColumns = getOrderedColumns(randomTable, maxColumns, preferPrefixOrder,
                preferSuffixOrder);
        List<IndexElement> elements = new ArrayList<>();
        boolean addedRequiredExpression = false;
        for (int i = 0; i < orderedColumns.size(); i++) {
            PostgresColumn column = orderedColumns.get(i);
            boolean forceExpression = !addedRequiredExpression && i == orderedColumns.size() - 1;
            IndexElement element = createIndexElement(globalState, randomTable, column, model, method, errors,
                    forceExpression);
            if (isExpressionModel(model) && isExpressionElement(model, forceExpression, i)) {
                addedRequiredExpression = true;
            }
            elements.add(element);
        }
        return elements;
    }

    private static boolean isExpressionElement(PostgresIndexModel model, boolean forceExpression, int index) {
        if (!forceExpression) {
            return false;
        }
        return model == PostgresIndexModel.PREFIX_EXPR || model == PostgresIndexModel.SUFFIX_EXPR
                || model == PostgresIndexModel.EXPRESSION || index == 0;
    }

    private static List<IndexElement> createHashIndexElements(PostgresTable randomTable) {
        List<IndexElement> elements = new ArrayList<>();
        elements.add(new IndexElement(randomTable.getRandomColumn().getName()));
        return elements;
    }

    private static IndexElement createIndexElement(PostgresGlobalState globalState, PostgresTable randomTable,
            PostgresColumn column, PostgresIndexModel model, IndexType method, ExpectedErrors errors,
            boolean forceExpression) {
        StringBuilder sb = new StringBuilder();
        if (shouldUseExpression(model, forceExpression)) {
            sb.append("(");
            sb.append(getExpressionSql(globalState, randomTable, column, model));
            sb.append(")");
        } else {
            sb.append(column.getName());
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(" ");
            sb.append(globalState.getRandomOpclass());
            errors.add("does not accept");
            errors.add("does not exist for access method");
        }
        if (method == IndexType.BTREE || method == IndexType.GIST) {
            if (Randomly.getBoolean()) {
                sb.append(" ");
                sb.append(Randomly.fromOptions("ASC", "DESC"));
            }
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" NULLS ");
                sb.append(Randomly.fromOptions("FIRST", "LAST"));
            }
        }
        return new IndexElement(sb.toString());
    }

    private static boolean shouldUseExpression(PostgresIndexModel model, boolean forceExpression) {
        switch (model) {
        case PREFIX_EXPR:
        case SUFFIX_EXPR:
        case EXPRESSION:
            return forceExpression || Randomly.getBoolean();
        default:
            return false;
        }
    }

    private static String getExpressionSql(PostgresGlobalState globalState, PostgresTable randomTable,
            PostgresColumn column, PostgresIndexModel model) {
        switch (model) {
        case PREFIX_EXPR:
            return PostgresVisitor.asString(createPrefixExpression(column));
        case SUFFIX_EXPR:
            return PostgresVisitor.asString(createSuffixExpression(column));
        case EXPRESSION:
            return PostgresVisitor.asString(
                    PostgresExpressionGenerator.generateExpression(globalState, randomTable.getColumns()));
        default:
            return PostgresVisitor.asString(
                    PostgresExpressionGenerator.generateExpression(globalState, randomTable.getColumns()));
        }
    }

    private static PostgresExpression createPrefixExpression(PostgresColumn column) {
        PostgresExpression columnReference = new PostgresColumnReference(column);
        switch (column.getType()) {
        case BOOLEAN:
            return new PostgresPrefixOperation(columnReference, PrefixOperator.NOT);
        case INT:
            return new PostgresPrefixOperation(columnReference,
                    Randomly.fromOptions(PrefixOperator.UNARY_PLUS, PrefixOperator.UNARY_MINUS));
        default:
            return new PostgresCastOperation(columnReference, PostgresCompoundDataType.create(PostgresDataType.TEXT));
        }
    }

    private static PostgresExpression createSuffixExpression(PostgresColumn column) {
        PostgresExpression columnReference = new PostgresColumnReference(column);
        if (Randomly.getBoolean()) {
            return new PostgresPostfixOperation(columnReference,
                    Randomly.fromOptions(PostfixOperator.IS_NULL, PostfixOperator.IS_NOT_NULL));
        }
        return new PostgresPostfixText(columnReference, "::text", null, PostgresDataType.TEXT);
    }

    private static List<PostgresColumn> getOrderedColumns(PostgresTable randomTable, int targetCount,
            boolean preferPrefixOrder, boolean preferSuffixOrder) {
        int nrColumns = Math.max(1, Math.min(targetCount, randomTable.getColumns().size()));
        List<PostgresColumn> columns = Randomly.nonEmptySubset(randomTable.getColumns(), nrColumns);
        if (columns.size() >= 2 && (preferPrefixOrder || preferSuffixOrder)) {
            PostgresColumn emphasized = Randomly.fromList(columns);
            columns.remove(emphasized);
            if (preferPrefixOrder) {
                columns.add(0, emphasized);
            } else {
                columns.add(emphasized);
            }
        }
        return columns;
    }

    private static IndexType chooseIndexType(PostgresIndexModel model, boolean unique) {
        if (unique) {
            return IndexType.BTREE;
        }
        switch (model) {
        case PREFIX_EXPR:
        case SUFFIX_EXPR:
        case EXPRESSION:
            return Randomly.fromOptions(IndexType.BTREE, IndexType.GIST);
        case COMPOSITE:
            return Randomly.fromOptions(IndexType.BTREE, IndexType.GIST, IndexType.GIN);
        case UNIQUE:
        case PRIMARY_KEY:
            throw new AssertionError(model);
        default:
            return Randomly.fromOptions(IndexType.BTREE, IndexType.HASH, IndexType.GIST, IndexType.GIN);
        }
    }

    private static boolean isExpressionModel(PostgresIndexModel model) {
        return model == PostgresIndexModel.PREFIX_EXPR || model == PostgresIndexModel.SUFFIX_EXPR
                || model == PostgresIndexModel.EXPRESSION;
    }

    private static boolean canUseInclude(IndexType method) {
        return method != IndexType.HASH;
    }

    private static boolean canUseWhereClause(PostgresIndexModel model) {
        return model != PostgresIndexModel.PRIMARY_KEY;
    }

    private static void addSharedIndexErrors(ExpectedErrors errors) {
        errors.add("already contains data");
        errors.add("You might need to add explicit type casts");
        errors.add(" collations are not supported");
        errors.add("because it has pending trigger events");
        errors.add("could not determine which collation to use for index expression");
        errors.add("could not determine which collation to use for string comparison");
        errors.add("is duplicated");
        errors.add("already exists");
        errors.add("has no default operator class");
        errors.add("does not support");
        errors.add("cannot cast");
        errors.add("invalid input syntax for");
        errors.add("must be type ");
        errors.add("integer out of range");
        errors.add("division by zero");
        errors.add("out of range");
        errors.add("functions in index predicate must be marked IMMUTABLE");
        errors.add("functions in index expression must be marked IMMUTABLE");
        errors.add("result of range difference would not be contiguous");
        errors.add("which is part of the partition key");
        PostgresCommon.addCommonExpressionErrors(errors);
    }

    private static void addUniqueErrors(ExpectedErrors errors) {
        errors.add("access method \"gin\" does not support unique indexes");
        errors.add("access method \"hash\" does not support unique indexes");
        errors.add("could not create unique index");
        errors.add("unsupported UNIQUE constraint with partition key definition");
        errors.add("insufficient columns in UNIQUE constraint definition");
    }

    private static void addPrimaryKeyErrors(ExpectedErrors errors) {
        errors.add("already exists");
        errors.add("multiple primary keys for table");
        errors.add("contains null values");
        errors.add("unsupported PRIMARY KEY constraint with partition key definition");
        errors.add("which is part of the partition key");
        errors.add("insufficient columns in PRIMARY KEY constraint definition");
        errors.add("could not create unique index");
    }

    private static String getNewIndexName(PostgresTable randomTable) {
        List<PostgresIndex> indexes = randomTable.getIndexes();
        int indexI = 0;
        while (true) {
            String indexName = DBMSCommon.createIndexName(indexI++);
            if (indexes.stream().noneMatch(i -> i.getIndexName().equals(indexName))) {
                return indexName;
            }
        }
    }

}
