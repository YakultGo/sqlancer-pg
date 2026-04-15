package sqlancer.postgres;

import java.sql.ResultSet;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.postgresql.util.PSQLException;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractRowValue;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresSchema.PostgresTable.TableType;
import sqlancer.postgres.ast.PostgresConstant;

public class PostgresSchema extends AbstractSchema<PostgresGlobalState, PostgresTable> {

    private final String databaseName;

    public enum PostgresDataType {
        INT, BOOLEAN, TEXT, DECIMAL, FLOAT, REAL, RANGE, MONEY, BIT, INET, DATE, TIME, TIMETZ, TIMESTAMP,
        TIMESTAMPTZ, INTERVAL, ARRAY;

        public static PostgresDataType getRandomType() {
            List<PostgresDataType> dataTypes = new ArrayList<>(Arrays.asList(values()));
            dataTypes.remove(PostgresDataType.ARRAY);
            if (PostgresProvider.generateOnlyKnown) {
                dataTypes.remove(PostgresDataType.DECIMAL);
                dataTypes.remove(PostgresDataType.FLOAT);
                dataTypes.remove(PostgresDataType.REAL);
                dataTypes.remove(PostgresDataType.INET);
                dataTypes.remove(PostgresDataType.RANGE);
                dataTypes.remove(PostgresDataType.MONEY);
                dataTypes.remove(PostgresDataType.BIT);
                dataTypes.remove(PostgresDataType.INTERVAL);
            }
            return Randomly.fromList(dataTypes);
        }
    }

    public static class PostgresColumn extends AbstractTableColumn<PostgresTable, PostgresDataType> {
        private final PostgresCompoundDataType compoundType;

        public PostgresColumn(String name, PostgresDataType columnType) {
            super(name, null, columnType);
            this.compoundType = PostgresCompoundDataType.create(columnType);
        }

        public PostgresColumn(String name, PostgresCompoundDataType compoundType) {
            super(name, null, compoundType.getDataType());
            this.compoundType = compoundType;
        }

        public static PostgresColumn createDummy(String name) {
            return new PostgresColumn(name, PostgresDataType.INT);
        }

        public PostgresCompoundDataType getCompoundType() {
            return compoundType;
        }

    }

    public static class PostgresTables extends AbstractTables<PostgresTable, PostgresColumn> {

        public PostgresTables(List<PostgresTable> tables) {
            super(tables);
        }

        public PostgresRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            String randomRow = String.format("SELECT %s FROM %s ORDER BY RANDOM() LIMIT 1", columnNamesAsString(
                    c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    // columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." +
                    // c.getName() + ")")
                    tableNamesAsString());
            Map<PostgresColumn, PostgresConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet randomRowValues = s.executeQuery(randomRow);
                if (!randomRowValues.next()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    PostgresColumn column = getColumns().get(i);
                    int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    PostgresConstant constant;
                    if (randomRowValues.getString(columnIndex) == null) {
                        constant = PostgresConstant.createNullConstant();
                    } else {
                        constant = getColumnValue(randomRowValues, columnIndex, column.getCompoundType());
                    }
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                return new PostgresRowValue(this, values);
            } catch (PSQLException e) {
                throw new IgnoreMeException();
            }

        }

    }

    public static PostgresDataType getColumnType(String typeString) {
        return getColumnCompoundType(typeString, null).getDataType();
    }

    public static PostgresCompoundDataType getColumnCompoundType(String typeString, String elementTypeString) {
        if (typeString == null) {
            throw new AssertionError("typeString must not be null");
        }
        String normalizedType = normalizeTypeName(typeString);
        if ("array".equals(normalizedType)) {
            if (elementTypeString == null) {
                throw new AssertionError(typeString);
            }
            PostgresCompoundDataType arrayType = PostgresCompoundDataType.createArray(
                    getColumnCompoundType(stripArrayUdtPrefix(elementTypeString), null));
            if (!arrayType.isSupportedArrayType()) {
                throw new IgnoreMeException();
            }
            return arrayType;
        }
        if (normalizedType.endsWith("[]")) {
            PostgresCompoundDataType arrayType = PostgresCompoundDataType
                    .createArray(getColumnCompoundType(normalizedType.substring(0, normalizedType.length() - 2), null));
            if (!arrayType.isSupportedArrayType()) {
                throw new IgnoreMeException();
            }
            return arrayType;
        }
        switch (normalizedType) {
        case "smallint":
        case "int2":
        case "integer":
        case "int4":
        case "bigint":
        case "int8":
            return PostgresCompoundDataType.create(PostgresDataType.INT);
        case "boolean":
        case "bool":
            return PostgresCompoundDataType.create(PostgresDataType.BOOLEAN);
        case "text":
        case "character":
        case "bpchar":
        case "character varying":
        case "varchar":
        case "name":
        case "regclass":
            return PostgresCompoundDataType.create(PostgresDataType.TEXT);
        case "numeric":
            return PostgresCompoundDataType.create(PostgresDataType.DECIMAL);
        case "double precision":
            return PostgresCompoundDataType.create(PostgresDataType.FLOAT);
        case "real":
            return PostgresCompoundDataType.create(PostgresDataType.REAL);
        case "int4range":
            return PostgresCompoundDataType.create(PostgresDataType.RANGE);
        case "money":
            return PostgresCompoundDataType.create(PostgresDataType.MONEY);
        case "bit":
        case "bit varying":
        case "varbit":
            return PostgresCompoundDataType.create(PostgresDataType.BIT);
        case "inet":
            return PostgresCompoundDataType.create(PostgresDataType.INET);
        case "date":
            return PostgresCompoundDataType.create(PostgresDataType.DATE);
        case "time without time zone":
        case "time":
            return PostgresCompoundDataType.create(PostgresDataType.TIME);
        case "time with time zone":
        case "timetz":
            return PostgresCompoundDataType.create(PostgresDataType.TIMETZ);
        case "timestamp without time zone":
        case "timestamp":
            return PostgresCompoundDataType.create(PostgresDataType.TIMESTAMP);
        case "timestamp with time zone":
        case "timestamptz":
            return PostgresCompoundDataType.create(PostgresDataType.TIMESTAMPTZ);
        case "interval":
            return PostgresCompoundDataType.create(PostgresDataType.INTERVAL);
        default:
            throw new AssertionError(typeString);
        }
    }

    private static String normalizeTypeName(String typeString) {
        String normalizedType = typeString.trim().toLowerCase();
        int parenIndex = normalizedType.indexOf('(');
        if (parenIndex != -1) {
            normalizedType = normalizedType.substring(0, parenIndex).trim();
        }
        return normalizedType;
    }

    public static boolean isSupportedArrayElementType(PostgresDataType type) {
        switch (type) {
        case INT:
        case BOOLEAN:
        case TEXT:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case TIMESTAMPTZ:
        case INTERVAL:
            return true;
        default:
            return false;
        }
    }

    private static String stripArrayUdtPrefix(String elementTypeString) {
        String normalizedType = elementTypeString.trim().toLowerCase();
        while (normalizedType.startsWith("_")) {
            normalizedType = normalizedType.substring(1);
        }
        return normalizedType;
    }

    private static PostgresConstant getColumnValue(ResultSet rs, int columnIndex, PostgresCompoundDataType compoundType)
            throws SQLException {
        if (compoundType.isArray()) {
            Array array = rs.getArray(columnIndex);
            if (array == null) {
                return PostgresConstant.createNullConstant();
            }
            return PostgresConstant.createArrayConstant(array, compoundType);
        }
        switch (compoundType.getDataType()) {
        case INT:
            return PostgresConstant.createIntConstant(rs.getLong(columnIndex));
        case BOOLEAN:
            return PostgresConstant.createBooleanConstant(rs.getBoolean(columnIndex));
        case TEXT:
            return PostgresConstant.createTextConstant(rs.getString(columnIndex));
        case DATE:
            return PostgresConstant.createDateConstant(rs.getString(columnIndex));
        case TIME:
            return PostgresConstant.createTimeConstant(rs.getString(columnIndex));
        case TIMETZ:
            return PostgresConstant.createTimeWithTimeZoneConstant(rs.getString(columnIndex));
        case TIMESTAMP:
            return PostgresConstant.createTimestampConstant(rs.getString(columnIndex));
        case TIMESTAMPTZ:
            return PostgresConstant.createTimestampWithTimeZoneConstant(rs.getString(columnIndex));
        case INTERVAL:
            return PostgresConstant.createIntervalConstant(rs.getString(columnIndex));
        default:
            throw new IgnoreMeException();
        }
    }

    public static class PostgresRowValue extends AbstractRowValue<PostgresTables, PostgresColumn, PostgresConstant> {

        protected PostgresRowValue(PostgresTables tables, Map<PostgresColumn, PostgresConstant> values) {
            super(tables, values);
        }

    }

    public static class PostgresTable
            extends AbstractRelationalTable<PostgresColumn, PostgresIndex, PostgresGlobalState> {

        public enum TableType {
            STANDARD, TEMPORARY
        }

        private final TableType tableType;
        private final List<PostgresStatisticsObject> statistics;
        private final boolean isInsertable;
        private final boolean isPartitioned;

        public PostgresTable(String tableName, List<PostgresColumn> columns, List<PostgresIndex> indexes,
                TableType tableType, List<PostgresStatisticsObject> statistics, boolean isView, boolean isInsertable) {
            super(tableName, columns, indexes, isView);
            this.statistics = statistics;
            this.isInsertable = isInsertable;
            this.tableType = tableType;
            // TODO: simple adapter for other implementations
            this.isPartitioned = false;
        }

        public PostgresTable(String tableName, List<PostgresColumn> columns, List<PostgresIndex> indexes,
                TableType tableType, List<PostgresStatisticsObject> statistics, boolean isView, boolean isInsertable,
                boolean isPartitioned) {
            super(tableName, columns, indexes, isView);
            this.statistics = statistics;
            this.isInsertable = isInsertable;
            this.tableType = tableType;
            this.isPartitioned = isPartitioned;
        }

        public List<PostgresStatisticsObject> getStatistics() {
            return statistics;
        }

        public TableType getTableType() {
            return tableType;
        }

        public boolean isInsertable() {
            return isInsertable;
        }

        public boolean isPartitioned() {
            return isPartitioned;
        }

    }

    public static final class PostgresStatisticsObject {
        private final String name;

        public PostgresStatisticsObject(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static final class PostgresIndex extends TableIndex {

        private PostgresIndex(String indexName) {
            super(indexName);
        }

        public static PostgresIndex create(String indexName) {
            return new PostgresIndex(indexName);
        }

        @Override
        public String getIndexName() {
            if (super.getIndexName().contentEquals("PRIMARY")) {
                return "`PRIMARY`";
            } else {
                return super.getIndexName();
            }
        }

    }

    public static PostgresSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        try {
            List<PostgresTable> databaseTables = new ArrayList<>();
            try (Statement s = con.createStatement()) {
                try (ResultSet rs = s.executeQuery(
                        "SELECT t.table_name, t.table_schema, t.table_type, t.is_insertable_into, c.relkind FROM information_schema.tables t JOIN pg_class c ON c.relname = t.table_name JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema WHERE t.table_schema='public' OR t.table_schema LIKE 'pg_temp_%' ORDER BY t.table_name;")) {
                    while (rs.next()) {
                        String tableName = rs.getString("table_name");
                        String tableTypeSchema = rs.getString("table_schema");
                        boolean isInsertable = rs.getBoolean("is_insertable_into");
                        boolean isPartitioned = "p".equals(rs.getString("relkind"));
                        // TODO: also check insertable
                        // TODO: insert into view?
                        boolean isView = tableName.startsWith("v"); // tableTypeStr.contains("VIEW") ||
                                                                    // tableTypeStr.contains("LOCAL TEMPORARY") &&
                                                                    // !isInsertable;
                        PostgresTable.TableType tableType = getTableType(tableTypeSchema);
                        List<PostgresColumn> databaseColumns = getTableColumns(con, tableName);
                        List<PostgresIndex> indexes = getIndexes(con, tableName);
                        List<PostgresStatisticsObject> statistics = getStatistics(con);
                        PostgresTable t = new PostgresTable(tableName, databaseColumns, indexes, tableType, statistics,
                                isView, isInsertable, isPartitioned);
                        for (PostgresColumn c : databaseColumns) {
                            c.setTable(t);
                        }
                        databaseTables.add(t);
                    }
                }
            }
            return new PostgresSchema(databaseTables, databaseName);
        } catch (SQLIntegrityConstraintViolationException e) {
            throw new AssertionError(e);
        }
    }

    protected static List<PostgresStatisticsObject> getStatistics(SQLConnection con) throws SQLException {
        List<PostgresStatisticsObject> statistics = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT stxname FROM pg_statistic_ext ORDER BY stxname;")) {
                while (rs.next()) {
                    statistics.add(new PostgresStatisticsObject(rs.getString("stxname")));
                }
            }
        }
        return statistics;
    }

    protected static PostgresTable.TableType getTableType(String tableTypeStr) throws AssertionError {
        PostgresTable.TableType tableType;
        if (tableTypeStr.contentEquals("public")) {
            tableType = TableType.STANDARD;
        } else if (tableTypeStr.startsWith("pg_temp")) {
            tableType = TableType.TEMPORARY;
        } else {
            throw new AssertionError(tableTypeStr);
        }
        return tableType;
    }

    protected static List<PostgresIndex> getIndexes(SQLConnection con, String tableName) throws SQLException {
        List<PostgresIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String
                    .format("SELECT indexname FROM pg_indexes WHERE tablename='%s' ORDER BY indexname;", tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("indexname");
                    if (DBMSCommon.matchesIndexName(indexName)) {
                        indexes.add(PostgresIndex.create(indexName));
                    }
                }
            }
        }
        return indexes;
    }

    protected static List<PostgresColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<PostgresColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(
                    "select c.column_name, c.data_type, c.udt_name, pg_catalog.format_type(a.atttypid, a.atttypmod) as formatted_type, a.attndims "
                            + "from INFORMATION_SCHEMA.COLUMNS c "
                            + "join pg_catalog.pg_namespace n on n.nspname = c.table_schema "
                            + "join pg_catalog.pg_class cl on cl.relname = c.table_name and cl.relnamespace = n.oid "
                            + "join pg_catalog.pg_attribute a on a.attrelid = cl.oid and a.attname = c.column_name "
                            + "where c.table_name = '" + tableName + "' and a.attnum > 0 and not a.attisdropped "
                            + "ORDER BY c.column_name")) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    String udtName = rs.getString("udt_name");
                    String formattedType = rs.getString("formatted_type");
                    int arrayDimensions = rs.getInt("attndims");
                    String elementType = null;
                    if ("array".equalsIgnoreCase(dataType) && udtName != null && udtName.startsWith("_")) {
                        elementType = udtName.substring(1);
                    }
                    PostgresCompoundDataType compoundType;
                    if ("array".equalsIgnoreCase(dataType) && arrayDimensions > 0 && elementType != null) {
                        compoundType = PostgresCompoundDataType
                                .createArray(getColumnCompoundType(stripArrayUdtPrefix(elementType), null), arrayDimensions);
                        if (!compoundType.isSupportedArrayType()) {
                            throw new IgnoreMeException();
                        }
                    } else {
                        String typeString = "array".equalsIgnoreCase(dataType) && formattedType != null ? formattedType : dataType;
                        compoundType = getColumnCompoundType(typeString, elementType);
                    }
                    PostgresColumn c = new PostgresColumn(columnName, compoundType);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    public PostgresSchema(List<PostgresTable> databaseTables, String databaseName) {
        super(databaseTables);
        this.databaseName = databaseName;
    }

    public PostgresTables getRandomTableNonEmptyTables() {
        return new PostgresTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public String getDatabaseName() {
        return databaseName;
    }

}
