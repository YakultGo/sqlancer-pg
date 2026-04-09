package sqlancer.postgres.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import sqlancer.Randomly;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresWindowFunction.WindowFrame;

public class PostgresSelect extends SelectBase<PostgresExpression>
        implements PostgresExpression, Select<PostgresJoin, PostgresExpression, PostgresTable, PostgresColumn> {

    private static final int FOR_CLAUSE_PROBABILITY = 50;

    private SelectType selectOption = SelectType.ALL;
    private List<PostgresJoin> joinClauses = Collections.emptyList();
    private PostgresExpression distinctOnClause;
    private ForClause forClause;
    private LockWaitOption lockWaitOption = LockWaitOption.NONE;
    private List<String> forClauseOfReferences = Collections.emptyList();
    private boolean allowForClause = true;
    private List<PostgresExpression> windowFunctions = new ArrayList<>();
    private final Map<String, WindowDefinition> windowDefinitions = new HashMap<>();

    public enum ForClause {
        UPDATE("UPDATE"), NO_KEY_UPDATE("NO KEY UPDATE"), SHARE("SHARE"), KEY_SHARE("KEY SHARE");

        private final String textRepresentation;

        ForClause(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public static ForClause getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum LockWaitOption {
        NONE(""), NOWAIT("NOWAIT"), SKIP_LOCKED("SKIP LOCKED");

        private final String textRepresentation;

        LockWaitOption(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public static LockWaitOption getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public static class WindowDefinition {
        private final List<PostgresExpression> partitionBy;
        private final List<PostgresOrderByTerm> orderBy;
        private final WindowFrame frame;

        public WindowDefinition(List<PostgresExpression> partitionBy, List<PostgresOrderByTerm> orderBy,
                WindowFrame frame) {
            this.partitionBy = partitionBy;
            this.orderBy = orderBy;
            this.frame = frame;
        }

        public List<PostgresExpression> getPartitionBy() {
            return partitionBy;
        }

        public List<PostgresOrderByTerm> getOrderBy() {
            return orderBy;
        }

        public WindowFrame getFrame() {
            return frame;
        }
    }

    // Getters setters for windowfunctions
    public List<PostgresExpression> getWindowFunctions() {
        return windowFunctions;
    }

    public void setWindowFunctions(List<PostgresExpression> windowFunctions) {
        this.windowFunctions = windowFunctions;
    }

    // Add methods for window definitions
    public void addWindowDefinition(String name, WindowDefinition definition) {
        windowDefinitions.put(name, definition);
    }

    public WindowDefinition getWindowDefinition(String name) {
        return windowDefinitions.get(name);
    }

    public Map<String, WindowDefinition> getWindowDefinitions() {
        return windowDefinitions;
    }

    public static class PostgresFromTable implements PostgresExpression {
        private final PostgresTable t;
        private final boolean only;

        public PostgresFromTable(PostgresTable t, boolean only) {
            this.t = t;
            this.only = only;
        }

        public PostgresTable getTable() {
            return t;
        }

        public boolean isOnly() {
            return only;
        }

        @Override
        public PostgresDataType getExpressionType() {
            return null;
        }
    }

    public static class PostgresSubquery implements PostgresExpression {
        private final PostgresSelect s;
        private final String name;

        public PostgresSubquery(PostgresSelect s, String name) {
            this.s = s;
            this.name = name;
        }

        public PostgresSelect getSelect() {
            return s;
        }

        public String getName() {
            return name;
        }

        @Override
        public PostgresDataType getExpressionType() {
            return null;
        }
    }

    public enum SelectType {
        DISTINCT, ALL;

        public static SelectType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public void setSelectType(SelectType fromOptions) {
        this.setSelectOption(fromOptions);
    }

    public void setDistinctOnClause(PostgresExpression distinctOnClause) {
        if (selectOption != SelectType.DISTINCT) {
            throw new IllegalArgumentException();
        }
        this.distinctOnClause = distinctOnClause;
    }

    public SelectType getSelectOption() {
        return selectOption;
    }

    public void setSelectOption(SelectType fromOptions) {
        this.selectOption = fromOptions;
    }

    @Override
    public PostgresDataType getExpressionType() {
        return null;
    }

    @Override
    public void setJoinClauses(List<PostgresJoin> joinStatements) {
        this.joinClauses = joinStatements;

    }

    @Override
    public List<PostgresJoin> getJoinClauses() {
        return joinClauses;
    }

    public PostgresExpression getDistinctOnClause() {
        return distinctOnClause;
    }

    public void setForClause(ForClause forClause) {
        this.forClause = forClause;
    }

    public ForClause getForClause() {
        return forClause;
    }

    public void setLockWaitOption(LockWaitOption lockWaitOption) {
        this.lockWaitOption = lockWaitOption;
    }

    public LockWaitOption getLockWaitOption() {
        return lockWaitOption;
    }

    public void setAllowForClause(boolean allowForClause) {
        this.allowForClause = allowForClause;
    }

    public boolean isAllowForClause() {
        return allowForClause;
    }

    public void maybeSetRandomForClause(boolean enabled) {
        maybeSetRandomForClause(enabled, List.of());
    }

    public void maybeSetRandomForClause(boolean enabled, List<String> lockableRefs) {
        if (!enabled || !shouldUseRandomForClause()) {
            return;
        }
        setForClause(ForClause.getRandom());
        setLockWaitOption(LockWaitOption.getRandom());
        maybeSetForClauseOfReferences(lockableRefs);
    }

    public void maybeSetForClauseOfReferences(List<String> lockableRefs) {
        if (forClause == null || !forClauseOfReferences.isEmpty() || !shouldUseForClauseOfReferences(lockableRefs)) {
            return;
        }
        List<String> distinctRefs = new ArrayList<>(new LinkedHashSet<>(lockableRefs));
        setForClauseOfReferences(getRandomForClauseOfReferences(distinctRefs));
    }

    private static boolean shouldUseRandomForClause() {
        return ThreadLocalRandom.current().nextInt(100) < FOR_CLAUSE_PROBABILITY;
    }

    private static boolean shouldUseForClauseOfReferences(List<String> lockableRefs) {
        if (lockableRefs.isEmpty()) {
            return false;
        }
        int probability = lockableRefs.size() >= 2 ? 45 : 25;
        return ThreadLocalRandom.current().nextInt(100) < probability;
    }

    private static List<String> getRandomForClauseOfReferences(List<String> distinctRefs) {
        if (distinctRefs.size() <= 1) {
            return distinctRefs;
        }
        if (distinctRefs.size() == 2) {
            return distinctRefs;
        }
        List<String> shuffledRefs = new ArrayList<>(distinctRefs);
        Collections.shuffle(shuffledRefs);
        if (ThreadLocalRandom.current().nextInt(100) < 80) {
            int nrRefs = ThreadLocalRandom.current().nextInt(2, distinctRefs.size() + 1);
            return new ArrayList<>(shuffledRefs.subList(0, nrRefs));
        }
        return Randomly.nonEmptySubset(shuffledRefs);
    }

    public void setForClauseOfReferences(List<String> forClauseOfReferences) {
        this.forClauseOfReferences = List.copyOf(forClauseOfReferences);
    }

    public List<String> getForClauseOfReferences() {
        return forClauseOfReferences;
    }

    @Override
    public String asString() {
        return PostgresVisitor.asString(this);
    }
}
