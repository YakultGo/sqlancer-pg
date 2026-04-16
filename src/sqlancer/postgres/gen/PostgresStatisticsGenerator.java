package sqlancer.postgres.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresStatisticsObject;
import sqlancer.postgres.PostgresSchema.PostgresTable;

public final class PostgresStatisticsGenerator {

    private PostgresStatisticsGenerator() {
    }

    public static SQLQueryAdapter insert(PostgresGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE STATISTICS ");
        if (Randomly.getBoolean()) {
            sb.append(" IF NOT EXISTS");
        }
        PostgresTable randomTable = globalState.getSchema().getRandomTable(t -> !t.isView()); // TODO materialized view
        if (randomTable.getColumns().size() < 2) {
            throw new IgnoreMeException();
        }
        sb.append(" ");
        sb.append(getNewStatisticsName(globalState));
        if (Randomly.getBoolean()) {
            sb.append(" (");
            List<String> statsSubset;
            statsSubset = Randomly.nonEmptySubset("ndistinct", "dependencies", "mcv");
            sb.append(statsSubset.stream().collect(Collectors.joining(", ")));
            sb.append(")");
        }

        List<PostgresColumn> randomColumns = randomTable.getRandomNonEmptyColumnSubset(
                globalState.getRandomly().getInteger(2, randomTable.getColumns().size()));
        sb.append(" ON ");
        sb.append(randomColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(" FROM ");
        sb.append(randomTable.getName());
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("cannot have more than 8 columns in statistics"),
                true);
    }

    public static SQLQueryAdapter remove(PostgresGlobalState globalState) {
        StringBuilder sb = new StringBuilder("DROP STATISTICS ");
        PostgresTable randomTable = globalState.getSchema().getRandomTable();
        List<PostgresStatisticsObject> statistics = randomTable.getStatistics();
        if (statistics.isEmpty()) {
            throw new IgnoreMeException();
        }
        sb.append(Randomly.fromList(statistics).getName());
        return new SQLQueryAdapter(sb.toString(), true);
    }

    public static SQLQueryAdapter alter(PostgresGlobalState globalState) {
        StringBuilder sb = new StringBuilder("ALTER STATISTICS ");
        PostgresTable randomTable = globalState.getSchema().getRandomTable();
        List<PostgresStatisticsObject> statistics = randomTable.getStatistics();
        if (statistics.isEmpty()) {
            throw new IgnoreMeException();
        }
        PostgresStatisticsObject randomStatistic = Randomly.fromList(statistics);
        sb.append(randomStatistic.getName());
        sb.append(" SET STATISTICS ");
        sb.append(Randomly.getNotCachedInteger(-1, 10000)); // -1 means default
        return new SQLQueryAdapter(sb.toString(), true);
    }

    private static String getNewStatisticsName(PostgresGlobalState globalState) {
        List<PostgresStatisticsObject> statistics = globalState.getSchema().getDatabaseTables().stream()
                .flatMap(t -> t.getStatistics().stream()).collect(Collectors.toList());
        int i = 0;
        while (true) {
            String candidateName = "s" + i;
            if (!statistics.stream().anyMatch(stat -> stat.getName().contentEquals(candidateName))) {
                return candidateName;
            }
            i++;
        }
    }

}
