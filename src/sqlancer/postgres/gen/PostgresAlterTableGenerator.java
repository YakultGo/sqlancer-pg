package sqlancer.postgres.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresCompoundDataType;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresConstraint;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresIndex;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresVisitor;

public class PostgresAlterTableGenerator {

    private final PostgresTable randomTable;
    private final Randomly r;
    private static PostgresColumn randomColumn;
    private final boolean generateOnlyKnown;
    private final List<String> collates;
    private final PostgresGlobalState globalState;

    protected enum Action {
        ADD_COLUMN,
        ALTER_TABLE_DROP_COLUMN,
        DROP_CONSTRAINT,
        ALTER_TABLE_RENAME_COLUMN,
        ALTER_COLUMN_TYPE,
        ALTER_COLUMN_SET_DROP_DEFAULT,
        ALTER_COLUMN_SET_DROP_NULL,
        ALTER_COLUMN_SET_STATISTICS,
        ALTER_COLUMN_SET_ATTRIBUTE_OPTION,
        ALTER_COLUMN_RESET_ATTRIBUTE_OPTION,
        ALTER_COLUMN_SET_STORAGE,
        ALTER_COLUMN_DROP_EXPRESSION,
        ADD_TABLE_CONSTRAINT,
        ADD_TABLE_CONSTRAINT_USING_INDEX,
        VALIDATE_CONSTRAINT,
        DISABLE_ROW_LEVEL_SECURITY,
        ENABLE_ROW_LEVEL_SECURITY,
        FORCE_ROW_LEVEL_SECURITY,
        NO_FORCE_ROW_LEVEL_SECURITY,
        CLUSTER_ON,
        SET_WITHOUT_CLUSTER,
        SET_WITH_OIDS,
        SET_WITHOUT_OIDS,
        SET_LOGGED_UNLOGGED,
        NOT_OF,
        OWNER_TO,
        REPLICA_IDENTITY,
        SET_STORAGE_PARAMETER,
        RESET_STORAGE_PARAMETER,
        ALTER_VIEW_RENAME_COLUMN
    }

    private enum Attribute {
        N_DISTINCT_INHERITED("n_distinct_inherited"), N_DISTINCT("n_distinct");

        private final String val;

        Attribute(String val) {
            this.val = val;
        }
    }

    private enum ActionTier {
        STABLE, REQUIRES_OBJECT, HIGH_RISK
    }

    private enum TableStorageParameter {
        FILLFACTOR("fillfactor", (r) -> r.getInteger(10, 100)),
        PARALLEL_WORKERS("parallel_workers", (r) -> r.getInteger(0, 1024)),
        AUTOVACUUM_ENABLED("autovacuum_enabled", (r) -> Randomly.fromOptions(0, 1)),
        AUTOVACUUM_VACUUM_THRESHOLD("autovacuum_vacuum_threshold", (r) -> r.getInteger(0, 2147483647)),
        AUTOVACUUM_VACUUM_SCALE_FACTOR("autovacuum_vacuum_scale_factor",
                (r) -> Randomly.fromOptions(0, 0.00001, 0.01, 0.1, 0.2, 0.5, 0.8, 0.9, 1)),
        AUTOVACUUM_ANALYZE_THRESHOLD("autovacuum_analyze_threshold", (r) -> r.getLong(0, Integer.MAX_VALUE)),
        AUTOVACUUM_ANALYZE_SCALE_FACTOR("autovacuum_analyze_scale_factor",
                (r) -> Randomly.fromOptions(0, 0.00001, 0.01, 0.1, 0.2, 0.5, 0.8, 0.9, 1)),
        AUTOVACUUM_VACUUM_COST_DELAY("autovacuum_vacuum_cost_delay", (r) -> r.getLong(0, 100)),
        AUTOVACUUM_VACUUM_COST_LIMIT("autovacuum_vacuum_cost_limit", (r) -> r.getLong(1, 10000)),
        AUTOVACUUM_FREEZE_MIN_AGE("autovacuum_freeze_min_age", (r) -> r.getLong(0, 1000000000)),
        AUTOVACUUM_FREEZE_MAX_AGE("autovacuum_freeze_max_age", (r) -> r.getLong(100000, 2000000000)),
        AUTOVACUUM_FREEZE_TABLE_AGE("autovacuum_freeze_table_age", (r) -> r.getLong(0, 2000000000));

        private final String parameter;
        private final java.util.function.Function<Randomly, Object> generator;

        TableStorageParameter(String parameter, java.util.function.Function<Randomly, Object> generator) {
            this.parameter = parameter;
            this.generator = generator;
        }
    }

    private static final List<Action> VIEW_ACTIONS = List.of(Action.ALTER_VIEW_RENAME_COLUMN);

    public PostgresAlterTableGenerator(PostgresTable randomTable, PostgresGlobalState globalState,
            boolean generateOnlyKnown) {
        this.randomTable = randomTable;
        this.globalState = globalState;
        this.r = globalState.getRandomly();
        this.generateOnlyKnown = generateOnlyKnown;
        this.collates = globalState.getCollates();
    }

    public static SQLQueryAdapter create(PostgresTable randomTable, PostgresGlobalState globalState,
            boolean generateOnlyKnown) {
        return new PostgresAlterTableGenerator(randomTable, globalState, generateOnlyKnown).generate();
    }

    public List<Action> getActions(ExpectedErrors errors) {
        PostgresCommon.addCommonExpressionErrors(errors);
        PostgresCommon.addCommonInsertUpdateErrors(errors);
        PostgresCommon.addCommonTableErrors(errors);
        errors.add("cannot drop desired object(s) because other objects depend on them");
        errors.add("invalid input syntax for");
        errors.add("it has pending trigger events");
        errors.add("could not open relation");
        errors.add("constraints on permanent tables may reference only permanent tables");

        if (randomTable.isView()) {
            return new ArrayList<>(VIEW_ACTIONS);
        }

        List<Action> candidates = new ArrayList<>();
        for (Action candidate : Action.values()) {
            if (VIEW_ACTIONS.contains(candidate)) {
                continue;
            }
            if (isActionSupported(candidate)) {
                candidates.add(candidate);
            }
        }
        if (candidates.isEmpty()) {
            throw new IgnoreMeException();
        }

        Action primary = Randomly.fromList(filterByTier(candidates, choosePrimaryTier(candidates)));
        List<Action> actions = new ArrayList<>();
        actions.add(primary);
        if (shouldAddCompanionAction(primary, candidates)) {
            List<Action> companions = getCompatibleCompanions(primary, candidates);
            if (!companions.isEmpty()) {
                actions.add(Randomly.fromList(companions));
            }
        }
        return actions;
    }

    private ActionTier choosePrimaryTier(List<Action> candidates) {
        List<Action> stableActions = filterByTier(candidates, ActionTier.STABLE);
        List<Action> objectActions = filterByTier(candidates, ActionTier.REQUIRES_OBJECT);
        List<Action> highRiskActions = filterByTier(candidates, ActionTier.HIGH_RISK);
        if (!stableActions.isEmpty() && !Randomly.getBooleanWithRatherLowProbability()) {
            return ActionTier.STABLE;
        }
        if (!objectActions.isEmpty() && Randomly.getBooleanWithRatherLowProbability()) {
            return ActionTier.REQUIRES_OBJECT;
        }
        if (!highRiskActions.isEmpty()) {
            return ActionTier.HIGH_RISK;
        }
        if (!objectActions.isEmpty()) {
            return ActionTier.REQUIRES_OBJECT;
        }
        return ActionTier.STABLE;
    }

    private List<Action> filterByTier(List<Action> candidates, ActionTier tier) {
        List<Action> filtered = new ArrayList<>();
        for (Action candidate : candidates) {
            if (getActionTier(candidate) == tier) {
                filtered.add(candidate);
            }
        }
        return filtered;
    }

    private boolean shouldAddCompanionAction(Action primary, List<Action> candidates) {
        if (Randomly.getBoolean()) {
            return false;
        }
        if (getActionTier(primary) == ActionTier.HIGH_RISK && !Randomly.getBooleanWithRatherLowProbability()) {
            return false;
        }
        return !getCompatibleCompanions(primary, candidates).isEmpty();
    }

    private List<Action> getCompatibleCompanions(Action primary, List<Action> candidates) {
        List<Action> companions = new ArrayList<>();
        for (Action candidate : candidates) {
            if (candidate == primary) {
                continue;
            }
            if (isCompatible(primary, candidate)) {
                companions.add(candidate);
            }
        }
        return companions;
    }

    private boolean isCompatible(Action first, Action second) {
        if (getActionTier(first) == ActionTier.HIGH_RISK || getActionTier(second) == ActionTier.HIGH_RISK) {
            return false;
        }
        if (isColumnAction(first) && isColumnAction(second)) {
            return false;
        }
        if (isStorageAction(first) && isStorageAction(second)) {
            return false;
        }
        if (first == Action.ADD_TABLE_CONSTRAINT || second == Action.ADD_TABLE_CONSTRAINT) {
            return false;
        }
        return true;
    }

    private boolean isColumnAction(Action action) {
        switch (action) {
        case ALTER_COLUMN_TYPE:
        case ALTER_COLUMN_SET_DROP_DEFAULT:
        case ALTER_COLUMN_SET_DROP_NULL:
        case ALTER_COLUMN_SET_STATISTICS:
        case ALTER_COLUMN_SET_ATTRIBUTE_OPTION:
        case ALTER_COLUMN_RESET_ATTRIBUTE_OPTION:
        case ALTER_COLUMN_SET_STORAGE:
        case ALTER_COLUMN_DROP_EXPRESSION:
        case ADD_COLUMN:
        case ALTER_TABLE_RENAME_COLUMN:
            return true;
        default:
            return false;
        }
    }

    private boolean isStorageAction(Action action) {
        switch (action) {
        case CLUSTER_ON:
        case SET_WITHOUT_CLUSTER:
        case SET_LOGGED_UNLOGGED:
        case SET_STORAGE_PARAMETER:
        case RESET_STORAGE_PARAMETER:
        case REPLICA_IDENTITY:
            return true;
        default:
            return false;
        }
    }

    private ActionTier getActionTier(Action action) {
        switch (action) {
        case ALTER_COLUMN_SET_DROP_DEFAULT:
        case ALTER_COLUMN_SET_DROP_NULL:
        case ALTER_COLUMN_SET_STATISTICS:
        case ALTER_COLUMN_SET_ATTRIBUTE_OPTION:
        case ALTER_COLUMN_RESET_ATTRIBUTE_OPTION:
        case ALTER_COLUMN_SET_STORAGE:
        case ALTER_COLUMN_DROP_EXPRESSION:
        case ADD_COLUMN:
        case DISABLE_ROW_LEVEL_SECURITY:
        case ENABLE_ROW_LEVEL_SECURITY:
        case FORCE_ROW_LEVEL_SECURITY:
        case NO_FORCE_ROW_LEVEL_SECURITY:
        case SET_WITHOUT_CLUSTER:
        case SET_WITHOUT_OIDS:
        case OWNER_TO:
        case NOT_OF:
        case SET_STORAGE_PARAMETER:
        case RESET_STORAGE_PARAMETER:
            return ActionTier.STABLE;
        case ADD_TABLE_CONSTRAINT_USING_INDEX:
        case VALIDATE_CONSTRAINT:
        case CLUSTER_ON:
        case DROP_CONSTRAINT:
            return ActionTier.REQUIRES_OBJECT;
        case ALTER_TABLE_DROP_COLUMN:
        case ALTER_TABLE_RENAME_COLUMN:
        case ALTER_COLUMN_TYPE:
        case ADD_TABLE_CONSTRAINT:
        case SET_LOGGED_UNLOGGED:
        case REPLICA_IDENTITY:
        case ALTER_VIEW_RENAME_COLUMN:
        case SET_WITH_OIDS:
            return ActionTier.HIGH_RISK;
        default:
            throw new AssertionError(action);
        }
    }

    private boolean isActionSupported(Action action) {
        switch (action) {
        case ADD_COLUMN:
            return true;
        case ALTER_TABLE_DROP_COLUMN:
            return randomTable.getColumns().size() > 1;
        case DROP_CONSTRAINT:
            return !randomTable.getConstraints().isEmpty();
        case ALTER_TABLE_RENAME_COLUMN:
            return true;
        case ALTER_COLUMN_TYPE:
            return !randomTable.isPartitioned() && randomTable.getTableType() != PostgresTable.TableType.TEMPORARY;
        case ADD_TABLE_CONSTRAINT_USING_INDEX:
            return !getCompatibleIndexesForConstraint().isEmpty();
        case VALIDATE_CONSTRAINT:
            return !getValidatableConstraints().isEmpty();
        case CLUSTER_ON:
            return !randomTable.getIndexes().isEmpty();
        case SET_WITH_OIDS:
            return false;
        case SET_LOGGED_UNLOGGED:
            return !randomTable.isPartitioned() && randomTable.getTableType() != PostgresTable.TableType.TEMPORARY;
        case REPLICA_IDENTITY:
            return true;
        case ALTER_VIEW_RENAME_COLUMN:
            return randomTable.isView();
        default:
            return true;
        }
    }

    private List<PostgresIndex> getCompatibleIndexesForConstraint() {
        List<PostgresIndex> indexes = new ArrayList<>();
        for (PostgresIndex index : randomTable.getIndexes()) {
            if (index.canBeUsedForAddConstraintUsingIndex()) {
                indexes.add(index);
            }
        }
        return indexes;
    }

    private List<PostgresConstraint> getValidatableConstraints() {
        List<PostgresConstraint> constraints = new ArrayList<>();
        for (PostgresConstraint constraint : randomTable.getConstraints()) {
            if (constraint.isValidatable()) {
                constraints.add(constraint);
            }
        }
        return constraints;
    }

    private List<PostgresIndex> getCompatibleIndexesForReplicaIdentity() {
        List<PostgresIndex> indexes = new ArrayList<>();
        for (PostgresIndex index : randomTable.getIndexes()) {
            if (index.canBeUsedForReplicaIdentity()) {
                indexes.add(index);
            }
        }
        return indexes;
    }

    public SQLQueryAdapter generate() {
        ExpectedErrors errors = new ExpectedErrors();
        List<Action> actions = getActions(errors);
        StringBuilder sb = new StringBuilder();

        if (randomTable.isView()) {
            sb.append("ALTER VIEW ");
        } else {
            sb.append("ALTER TABLE ");
            if (Randomly.getBoolean() && !actions.contains(Action.ADD_TABLE_CONSTRAINT)) {
                sb.append(" ONLY");
                errors.add("cannot use ONLY for foreign key on partitioned table");
            }
        }

        sb.append(" ");
        sb.append(randomTable.getName());
        sb.append(" ");

        int i = 0;
        for (Action action : actions) {
            if (i++ != 0) {
                sb.append(", ");
            }
            appendAction(sb, action, errors);
        }

        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private void appendAction(StringBuilder sb, Action action, ExpectedErrors errors) {
        switch (action) {
        case ADD_COLUMN:
            appendAddColumn(sb, errors);
            break;
        case ALTER_TABLE_DROP_COLUMN:
            sb.append("DROP ");
            if (Randomly.getBoolean()) {
                sb.append(" IF EXISTS ");
            }
            sb.append(randomTable.getRandomColumn().getName());
            errors.add("because other objects depend on it");
            if (Randomly.getBoolean()) {
                sb.append(" ");
                sb.append(Randomly.fromOptions("RESTRICT", "CASCADE"));
            }
            errors.add("does not exist");
            errors.add("cannot drop column");
            errors.add("cannot drop inherited column");
            break;
        case DROP_CONSTRAINT:
            appendDropConstraint(sb, errors);
            break;
        case ALTER_TABLE_RENAME_COLUMN:
            appendTableRenameColumn(sb, errors);
            break;
        case ALTER_COLUMN_TYPE:
            alterColumn(randomTable, sb);
            if (Randomly.getBoolean()) {
                sb.append(" SET DATA");
            }
            sb.append(" TYPE ");
            PostgresCompoundDataType randomType = getRandomCompoundType();
            PostgresCommon.appendDataType(randomType, sb, false, generateOnlyKnown, collates);
            errors.add("cannot alter type of a column used by a view or rule");
            errors.add("cannot convert infinity to numeric");
            errors.add("is duplicated");
            errors.add("cannot be cast automatically");
            errors.add("is an identity column");
            errors.add("identity column type must be smallint, integer, or bigint");
            errors.add("out of range");
            errors.add("cannot alter type of column named in partition key");
            errors.add("cannot alter type of column referenced in partition key expression");
            errors.add("because it is part of the partition key of relation");
            errors.add("argument of CHECK must be type boolean");
            errors.add("operator does not exist");
            errors.add("must be type");
            errors.add("You might need to add explicit type casts");
            errors.add("cannot cast type");
            errors.add("foreign key constrain");
            errors.add("division by zero");
            errors.add("value too long for type character varying");
            errors.add("cannot drop index");
            errors.add("cannot alter inherited column");
            errors.add("must be changed in child tables too");
            errors.add("could not determine which collation to use for index expression");
            errors.add("bit string too long for type bit varying");
            errors.add("cannot alter type of a column used by a generated column");
            errors.add("could not find cast from");
            break;
        case ALTER_COLUMN_SET_DROP_DEFAULT:
            alterColumn(randomTable, sb);
            if (Randomly.getBoolean()) {
                sb.append("DROP DEFAULT");
            } else {
                sb.append("SET DEFAULT ");
                sb.append(PostgresVisitor.asString(
                        PostgresExpressionGenerator.generateExpression(globalState, randomColumn.getCompoundType())));
                errors.add("is out of range");
                errors.add("but default expression is of type");
                errors.add("cannot cast");
            }
            errors.add("is a generated column");
            errors.add("is an identity column");
            break;
        case ALTER_COLUMN_SET_DROP_NULL:
            alterColumn(randomTable, sb);
            if (Randomly.getBoolean()) {
                sb.append("SET NOT NULL");
                errors.add("contains null values");
            } else {
                sb.append("DROP NOT NULL");
                errors.add("is in a primary key");
                errors.add("is an identity column");
                errors.add("is in index used as replica identity");
                errors.add("cannot drop inherited constraint");
            }
            break;
        case ALTER_COLUMN_SET_STATISTICS:
            alterColumn(randomTable, sb);
            sb.append("SET STATISTICS ");
            sb.append(r.getInteger(0, 10000));
            break;
        case ALTER_COLUMN_SET_ATTRIBUTE_OPTION:
            alterColumn(randomTable, sb);
            sb.append(" SET(");
            List<Attribute> subset = Randomly.nonEmptySubset(Attribute.values());
            appendAttributes(sb, subset, true);
            sb.append(")");
            break;
        case ALTER_COLUMN_RESET_ATTRIBUTE_OPTION:
            alterColumn(randomTable, sb);
            sb.append(" RESET(");
            appendAttributes(sb, Randomly.nonEmptySubset(Attribute.values()), false);
            sb.append(")");
            break;
        case ALTER_COLUMN_SET_STORAGE:
            alterColumn(randomTable, sb);
            sb.append("SET STORAGE ");
            sb.append(Randomly.fromOptions("PLAIN", "EXTERNAL", "EXTENDED", "MAIN"));
            errors.add("can only have storage");
            errors.add("is an identity column");
            break;
        case ALTER_COLUMN_DROP_EXPRESSION:
            alterColumn(randomTable, sb);
            sb.append("DROP EXPRESSION");
            if (Randomly.getBoolean()) {
                sb.append(" IF EXISTS");
            }
            errors.add("is not a generated column");
            errors.add("is not a stored generated column");
            errors.add("cannot drop expression from inherited column");
            errors.add("cannot drop generation expression from inherited column");
            errors.add("must be applied to child tables too");
            errors.add("cannot drop expression from column");
            break;
        case ADD_TABLE_CONSTRAINT:
            appendAddConstraint(sb, errors);
            break;
        case ADD_TABLE_CONSTRAINT_USING_INDEX:
            appendConstraintUsingIndex(sb, errors);
            break;
        case VALIDATE_CONSTRAINT:
            appendValidateConstraint(sb, errors);
            break;
        case DISABLE_ROW_LEVEL_SECURITY:
            sb.append("DISABLE ROW LEVEL SECURITY");
            break;
        case ENABLE_ROW_LEVEL_SECURITY:
            sb.append("ENABLE ROW LEVEL SECURITY");
            break;
        case FORCE_ROW_LEVEL_SECURITY:
            sb.append("FORCE ROW LEVEL SECURITY");
            break;
        case NO_FORCE_ROW_LEVEL_SECURITY:
            sb.append("NO FORCE ROW LEVEL SECURITY");
            break;
        case CLUSTER_ON:
            sb.append("CLUSTER ON ");
            sb.append(Randomly.fromList(randomTable.getIndexes()).getIndexName());
            errors.add("cannot cluster on");
            errors.add("cannot mark index clustered in partitioned table");
            errors.add("not valid");
            break;
        case SET_WITHOUT_CLUSTER:
            sb.append("SET WITHOUT CLUSTER");
            errors.add("cannot mark index clustered in partitioned table");
            break;
        case SET_WITH_OIDS:
            errors.add("is an identity column");
            sb.append("SET WITH OIDS");
            break;
        case SET_WITHOUT_OIDS:
            sb.append("SET WITHOUT OIDS");
            break;
        case SET_LOGGED_UNLOGGED:
            sb.append("SET ");
            sb.append(Randomly.fromOptions("LOGGED", "UNLOGGED"));
            errors.add("because it is temporary");
            errors.add("to logged because it references unlogged table");
            errors.add("to unlogged because it references logged table");
            break;
        case NOT_OF:
            errors.add("is not a typed table");
            sb.append("NOT OF");
            break;
        case OWNER_TO:
            sb.append("OWNER TO ");
            sb.append(Randomly.fromOptions("CURRENT_USER", "SESSION_USER"));
            break;
        case REPLICA_IDENTITY:
            appendReplicaIdentity(sb, errors);
            break;
        case SET_STORAGE_PARAMETER:
            appendTableStorageParameterChange(sb, errors, false);
            break;
        case RESET_STORAGE_PARAMETER:
            appendTableStorageParameterChange(sb, errors, true);
            break;
        case ALTER_VIEW_RENAME_COLUMN:
            appendViewRename(sb, errors);
            break;
        default:
            throw new AssertionError(action);
        }
    }

    private void appendAttributes(StringBuilder sb, List<Attribute> attributes, boolean includeValues) {
        int i = 0;
        for (Attribute attr : attributes) {
            if (i++ != 0) {
                sb.append(", ");
            }
            sb.append(attr.val);
            if (includeValues) {
                sb.append("=");
                sb.append(Randomly.fromOptions(-1, -0.8, -0.5, -0.2, -0.1, -0.00001, -0.0000000001, 0,
                        0.000000001, 0.0001, 0.1, 1));
            }
        }
    }

    private void appendAddConstraint(StringBuilder sb, ExpectedErrors errors) {
        sb.append("ADD ");
        sb.append("CONSTRAINT ");
        sb.append(randomTable.getFreeColumnName());
        sb.append(" ");
        PostgresCommon.addTableConstraint(sb, randomTable, globalState, errors);
        errors.add("functions in index expression must be marked IMMUTABLE");
        errors.add("functions in index predicate must be marked IMMUTABLE");
        errors.add("has no default operator class for access method");
        errors.add("does not accept data type");
        errors.add("does not exist for access method");
        errors.add("already exists");
        errors.add("multiple primary keys for table");
        errors.add("could not create unique index");
        errors.add("contains null values");
        errors.add("cannot cast type");
        errors.add("unsupported PRIMARY KEY constraint with partition key definition");
        errors.add("unsupported UNIQUE constraint with partition key definition");
        errors.add("insufficient columns in UNIQUE constraint definition");
        errors.add("which is part of the partition key");
        errors.add("out of range");
        errors.add("there is no unique constraint matching given keys for referenced table");
        errors.add("constraints on temporary tables may reference only temporary tables");
        errors.add("constraints on unlogged tables may reference only permanent or unlogged tables");
        errors.add("constraints on permanent tables may reference only permanent tables");
        errors.add("cannot reference partitioned table");
        errors.add("cannot be implemented");
        errors.add("violates foreign key constraint");
        errors.add("unsupported ON COMMIT and foreign key combination");
        errors.add("USING INDEX is not supported on partitioned tables");
        if (Randomly.getBoolean()) {
            sb.append(" NOT VALID");
            errors.add("cannot be marked NOT VALID");
            errors.add("cannot add NOT VALID foreign key on partitioned table");
        } else {
            errors.add("is violated by some row");
        }
    }

    private void appendAddColumn(StringBuilder sb, ExpectedErrors errors) {
        String columnName = randomTable.getFreeColumnName();
        PostgresCompoundDataType type = getRandomCompoundType();
        sb.append("ADD COLUMN ");
        sb.append(columnName);
        sb.append(" ");
        PostgresCommon.appendDataType(type, sb, false, generateOnlyKnown, collates);
        if (Randomly.getBoolean()) {
            sb.append(" DEFAULT ");
            sb.append(PostgresVisitor.asString(PostgresExpressionGenerator.generateExpression(globalState, type)));
            errors.add("but default expression is of type");
            errors.add("cannot cast");
            errors.add("is out of range");
        }
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("NULL", "NOT NULL"));
            errors.add("contains null values");
        }
        errors.add("column \"" + columnName + "\" of relation");
        errors.add("already exists");
        errors.add("cannot use column reference in DEFAULT expression");
        errors.add("cannot add column");
        errors.add("cannot add a column to a partition");
    }

    private void appendDropConstraint(StringBuilder sb, ExpectedErrors errors) {
        sb.append("DROP CONSTRAINT ");
        if (Randomly.getBoolean()) {
            sb.append("IF EXISTS ");
        }
        sb.append(Randomly.fromList(randomTable.getConstraints()).getName());
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("RESTRICT", "CASCADE"));
        }
        errors.add("does not exist");
        errors.add("cannot drop inherited constraint");
        errors.add("cannot drop constraint");
        errors.add("because other objects depend on it");
    }

    private void appendTableRenameColumn(StringBuilder sb, ExpectedErrors errors) {
        sb.append("RENAME COLUMN ");
        PostgresColumn columnToRename = randomTable.getRandomColumn();
        sb.append(columnToRename.getName());
        sb.append(" TO ");
        sb.append("new_");
        sb.append(columnToRename.getName());
        sb.append("_");
        sb.append(r.getInteger(1, 1000));
        errors.add("column does not exist");
        errors.add("column name already exists");
        errors.add("cannot rename inherited column");
        errors.add("cannot rename column");
    }

    private void appendConstraintUsingIndex(StringBuilder sb, ExpectedErrors errors) {
        List<PostgresIndex> indexes = getCompatibleIndexesForConstraint();
        if (indexes.isEmpty()) {
            throw new IgnoreMeException();
        }
        sb.append("ADD CONSTRAINT ");
        sb.append(randomTable.getFreeColumnName());
        sb.append(" ");
        sb.append(Randomly.fromOptions("UNIQUE", "PRIMARY KEY"));
        sb.append(" USING INDEX ");
        sb.append(Randomly.fromList(indexes).getIndexName());
        errors.add("already exists");
        errors.add("not valid");
        errors.add("is not a unique index");
        errors.add("is already associated with a constraint");
        errors.add("Cannot create a primary key or unique constraint using such an index");
        errors.add("multiple primary keys for table");
        errors.add("appears twice in unique constraint");
        errors.add("appears twice in primary key constraint");
        errors.add("contains null values");
        errors.add("insufficient columns in PRIMARY KEY constraint definition");
        errors.add("which is part of the partition key");
    }

    private void appendValidateConstraint(StringBuilder sb, ExpectedErrors errors) {
        List<PostgresConstraint> constraints = getValidatableConstraints();
        if (constraints.isEmpty()) {
            throw new IgnoreMeException();
        }
        sb.append("VALIDATE CONSTRAINT ");
        sb.append(Randomly.fromList(constraints).getName());
        errors.add("is violated by some row");
        errors.add("does not exist");
    }

    private void appendReplicaIdentity(StringBuilder sb, ExpectedErrors errors) {
        sb.append("REPLICA IDENTITY ");
        List<PostgresIndex> usableIndexes = getCompatibleIndexesForReplicaIdentity();
        if (usableIndexes.isEmpty() || Randomly.getBoolean()) {
            sb.append(Randomly.fromOptions("DEFAULT", "FULL", "NOTHING"));
        } else {
            sb.append("USING INDEX ");
            sb.append(Randomly.fromList(usableIndexes).getIndexName());
            errors.add("cannot be used as replica identity");
            errors.add("cannot use non-unique index");
            errors.add("cannot use expression index");
            errors.add("cannot use partial index");
            errors.add("cannot use invalid index");
        }
    }

    private void appendTableStorageParameterChange(StringBuilder sb, ExpectedErrors errors, boolean reset) {
        errors.add("unrecognized parameter");
        List<TableStorageParameter> parameters = Randomly.nonEmptySubset(TableStorageParameter.values());
        if (reset) {
            sb.append("RESET (");
        } else {
            sb.append("SET (");
        }
        int i = 0;
        for (TableStorageParameter parameter : parameters) {
            if (i++ != 0) {
                sb.append(", ");
            }
            sb.append(parameter.parameter);
            if (!reset) {
                sb.append("=");
                sb.append(parameter.generator.apply(globalState.getRandomly()));
            }
        }
        sb.append(")");
    }

    private void appendViewRename(StringBuilder sb, ExpectedErrors errors) {
        sb.append("RENAME COLUMN ");
        PostgresColumn columnToRename = randomTable.getRandomColumn();
        sb.append(columnToRename.getName());
        sb.append(" TO ");
        sb.append("new_");
        sb.append(columnToRename.getName());
        sb.append("_");
        sb.append(r.getInteger(1, 1000));
        errors.add("column does not exist");
        errors.add("column name already exists");
        errors.add("cannot rename column of view");
        errors.add("permission denied");
    }

    private PostgresCompoundDataType getRandomCompoundType() {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return PostgresExpressionGenerator.getRandomArrayType(Randomly.getBoolean() ? 1 : 2);
        }
        return PostgresCompoundDataType.create(PostgresDataType.getRandomType());
    }

    private static void alterColumn(PostgresTable randomTable, StringBuilder sb) {
        sb.append("ALTER ");
        randomColumn = randomTable.getRandomColumn();
        sb.append(randomColumn.getName());
        sb.append(" ");
    }

}
