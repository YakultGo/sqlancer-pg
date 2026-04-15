package sqlancer.postgres;

import java.util.Objects;
import java.util.Optional;

import sqlancer.postgres.PostgresSchema.PostgresDataType;

public final class PostgresCompoundDataType {

    public static final int MAX_ARRAY_DIMENSIONS = 3;

    private final PostgresDataType dataType;
    private final PostgresCompoundDataType elemType;
    private final Integer size;

    private PostgresCompoundDataType(PostgresDataType dataType, PostgresCompoundDataType elemType, Integer size) {
        this.dataType = dataType;
        this.elemType = elemType;
        this.size = size;
    }

    public PostgresDataType getDataType() {
        return dataType;
    }

    public boolean isArray() {
        return dataType == PostgresDataType.ARRAY;
    }

    public int getArrayDimensions() {
        if (!isArray()) {
            return 0;
        }
        return 1 + elemType.getArrayDimensions();
    }

    public PostgresCompoundDataType getArrayBaseElementType() {
        PostgresCompoundDataType current = this;
        while (current.isArray()) {
            current = current.getElemType();
        }
        return current;
    }

    public boolean isSupportedArrayType() {
        return isArray() && getArrayDimensions() <= MAX_ARRAY_DIMENSIONS
                && PostgresSchema.isSupportedArrayElementType(getArrayBaseElementType().getDataType());
    }

    public PostgresCompoundDataType getElemType() {
        if (elemType == null) {
            throw new AssertionError();
        }
        return elemType;
    }

    public Optional<Integer> getSize() {
        if (size == null) {
            return Optional.empty();
        } else {
            return Optional.of(size);
        }
    }

    public static PostgresCompoundDataType create(PostgresDataType type, int size) {
        return new PostgresCompoundDataType(type, null, size);
    }

    public static PostgresCompoundDataType create(PostgresDataType type) {
        return new PostgresCompoundDataType(type, null, null);
    }

    public static PostgresCompoundDataType createArray(PostgresCompoundDataType elementType) {
        return new PostgresCompoundDataType(PostgresDataType.ARRAY, Objects.requireNonNull(elementType), null);
    }

    public static PostgresCompoundDataType createArray(PostgresDataType elementType) {
        return createArray(create(elementType));
    }

    public static PostgresCompoundDataType createArray(PostgresCompoundDataType elementType, int dimensions) {
        if (dimensions <= 0) {
            throw new AssertionError(dimensions);
        }
        PostgresCompoundDataType type = Objects.requireNonNull(elementType);
        for (int i = 0; i < dimensions; i++) {
            type = createArray(type);
        }
        return type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataType, elemType, size);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PostgresCompoundDataType)) {
            return false;
        }
        PostgresCompoundDataType other = (PostgresCompoundDataType) obj;
        return dataType == other.dataType && Objects.equals(elemType, other.elemType) && Objects.equals(size, other.size);
    }

    @Override
    public String toString() {
        if (isArray()) {
            return elemType + "[]";
        }
        if (size == null) {
            return dataType.toString();
        }
        return dataType + "(" + size + ")";
    }
}
