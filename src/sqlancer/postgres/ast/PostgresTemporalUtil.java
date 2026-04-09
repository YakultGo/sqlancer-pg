package sqlancer.postgres.ast;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sqlancer.IgnoreMeException;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

final class PostgresTemporalUtil {

    enum TemporalField {
        YEAR, MONTH, DAY, HOUR, MINUTE, SECOND;

        static TemporalField fromString(String text) {
            switch (text.trim().toLowerCase()) {
            case "year":
                return YEAR;
            case "month":
            case "mon":
                return MONTH;
            case "day":
                return DAY;
            case "hour":
                return HOUR;
            case "minute":
            case "min":
                return MINUTE;
            case "second":
            case "sec":
                return SECOND;
            default:
                throw new IgnoreMeException();
            }
        }
    }

    static final class IntervalValue implements Comparable<IntervalValue> {

        private final int months;
        private final int days;
        private final long nanos;

        IntervalValue(int months, int days, long nanos) {
            this.months = months;
            this.days = days;
            this.nanos = nanos;
        }

        int getMonths() {
            return months;
        }

        int getDays() {
            return days;
        }

        long getNanos() {
            return nanos;
        }

        IntervalValue plus(IntervalValue other) {
            return new IntervalValue(months + other.months, days + other.days, nanos + other.nanos);
        }

        IntervalValue minus(IntervalValue other) {
            return new IntervalValue(months - other.months, days - other.days, nanos - other.nanos);
        }

        @Override
        public int compareTo(IntervalValue other) {
            int monthComparison = Integer.compare(months, other.months);
            if (monthComparison != 0) {
                return monthComparison;
            }
            int dayComparison = Integer.compare(days, other.days);
            if (dayComparison != 0) {
                return dayComparison;
            }
            return Long.compare(nanos, other.nanos);
        }

        String toCanonicalString() {
            StringBuilder sb = new StringBuilder();
            int years = months / 12;
            int remainingMonths = months % 12;
            if (years != 0) {
                sb.append(years).append(" years");
            }
            if (remainingMonths != 0) {
                appendWithSpace(sb, remainingMonths + " mons");
            }
            if (days != 0 || sb.length() == 0 && nanos == 0) {
                appendWithSpace(sb, days + " days");
            }
            if (nanos != 0 || sb.length() == 0) {
                appendWithSpace(sb, formatTimeNanos(nanos));
            }
            return sb.toString().trim();
        }

        private static void appendWithSpace(StringBuilder sb, String fragment) {
            if (sb.length() != 0) {
                sb.append(' ');
            }
            sb.append(fragment);
        }
    }

    private static final Pattern TRAILING_OFFSET_PATTERN = Pattern.compile("([+-])(\\d{2})(?::?(\\d{2}))?$");
    private static final Pattern UNIT_PATTERN = Pattern
            .compile("([+-]?\\d+)\\s+(years?|yrs?|mons?|months?|days?)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TIME_PATTERN = Pattern.compile("([+-])?(\\d{1,2}):(\\d{2}):(\\d{2})(?:\\.(\\d{1,9}))?");

    private static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder().appendPattern("HH:mm:ss")
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd().toFormatter();
    private static final DateTimeFormatter OFFSET_TIME_FORMATTER = new DateTimeFormatterBuilder().append(TIME_FORMATTER)
            .appendOffset("+HH:MM", "Z").toFormatter();
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss").optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd().toFormatter();

    private PostgresTemporalUtil() {
    }

    static LocalDate parseDate(String value) {
        try {
            return LocalDate.parse(value.trim());
        } catch (DateTimeParseException e) {
            throw new IgnoreMeException();
        }
    }

    static LocalTime parseTime(String value) {
        try {
            return LocalTime.parse(value.trim(), TIME_FORMATTER);
        } catch (DateTimeParseException e) {
            throw new IgnoreMeException();
        }
    }

    static OffsetTime parseOffsetTime(String value) {
        try {
            return OffsetTime.parse(normalizeOffset(value), OFFSET_TIME_FORMATTER);
        } catch (DateTimeParseException e) {
            throw new IgnoreMeException();
        }
    }

    static LocalDateTime parseTimestamp(String value) {
        try {
            return LocalDateTime.parse(normalizeTimestampSeparator(value), TIMESTAMP_FORMATTER);
        } catch (DateTimeParseException e) {
            throw new IgnoreMeException();
        }
    }

    static OffsetDateTime parseOffsetDateTime(String value) {
        try {
            return OffsetDateTime.parse(normalizeOffset(normalizeTimestampSeparator(value)).replace(" ", "T"));
        } catch (DateTimeParseException e) {
            throw new IgnoreMeException();
        }
    }

    static IntervalValue parseInterval(String value) {
        String input = value.trim();
        if (input.isEmpty()) {
            throw new IgnoreMeException();
        }
        int months = 0;
        int days = 0;
        long nanos = 0;
        Matcher unitMatcher = UNIT_PATTERN.matcher(input);
        StringBuffer remainderBuffer = new StringBuffer();
        while (unitMatcher.find()) {
            int amount = Integer.parseInt(unitMatcher.group(1));
            String unit = unitMatcher.group(2).toLowerCase();
            if (unit.startsWith("year") || unit.startsWith("yr")) {
                months += amount * 12;
            } else if (unit.startsWith("mon")) {
                months += amount;
            } else if (unit.startsWith("day")) {
                days += amount;
            } else {
                throw new IgnoreMeException();
            }
            unitMatcher.appendReplacement(remainderBuffer, " ");
        }
        unitMatcher.appendTail(remainderBuffer);
        String remainder = remainderBuffer.toString().trim();
        if (!remainder.isEmpty()) {
            Matcher timeMatcher = TIME_PATTERN.matcher(remainder);
            if (!timeMatcher.matches()) {
                throw new IgnoreMeException();
            }
            int sign = "-".equals(timeMatcher.group(1)) ? -1 : 1;
            int hours = Integer.parseInt(timeMatcher.group(2));
            int minutes = Integer.parseInt(timeMatcher.group(3));
            int seconds = Integer.parseInt(timeMatcher.group(4));
            String fractional = timeMatcher.group(5);
            int nanoPart = 0;
            if (fractional != null) {
                String normalizedFraction = (fractional + "000000000").substring(0, 9);
                nanoPart = Integer.parseInt(normalizedFraction);
            }
            nanos = sign
                    * (((hours * 60L + minutes) * 60L + seconds) * 1_000_000_000L + nanoPart);
        }
        return new IntervalValue(months, days, nanos);
    }

    static String asText(PostgresDataType type, String value) {
        switch (type) {
        case DATE:
            return formatDate(parseDate(value));
        case TIME:
            return formatTime(parseTime(value));
        case TIMETZ:
            return formatOffsetTime(parseOffsetTime(value));
        case TIMESTAMP:
            return formatTimestamp(parseTimestamp(value));
        case TIMESTAMPTZ:
            return formatOffsetTimestamp(parseOffsetDateTime(value));
        case INTERVAL:
            return parseInterval(value).toCanonicalString();
        default:
            throw new IgnoreMeException();
        }
    }

    static int compare(PostgresDataType type, String left, String right) {
        switch (type) {
        case DATE:
            return parseDate(left).compareTo(parseDate(right));
        case TIME:
            return parseTime(left).compareTo(parseTime(right));
        case TIMETZ:
            return parseOffsetTime(left).compareTo(parseOffsetTime(right));
        case TIMESTAMP:
            return parseTimestamp(left).compareTo(parseTimestamp(right));
        case TIMESTAMPTZ:
            return parseOffsetDateTime(left).compareTo(parseOffsetDateTime(right));
        case INTERVAL:
            return parseInterval(left).compareTo(parseInterval(right));
        default:
            throw new IgnoreMeException();
        }
    }

    static String addInterval(PostgresDataType temporalType, String temporalValue, String intervalValue) {
        IntervalValue interval = parseInterval(intervalValue);
        switch (temporalType) {
        case DATE:
            return formatTimestamp(applyInterval(parseDate(temporalValue).atStartOfDay(), interval));
        case TIME:
            return formatTime(applyInterval(parseTime(temporalValue), interval));
        case TIMETZ:
            return formatOffsetTime(applyInterval(parseOffsetTime(temporalValue), interval));
        case TIMESTAMP:
            return formatTimestamp(applyInterval(parseTimestamp(temporalValue), interval));
        case TIMESTAMPTZ:
            return formatOffsetTimestamp(applyInterval(parseOffsetDateTime(temporalValue), interval));
        default:
            throw new IgnoreMeException();
        }
    }

    static String subtractInterval(PostgresDataType temporalType, String temporalValue, String intervalValue) {
        return addInterval(temporalType, temporalValue, negate(parseInterval(intervalValue)).toCanonicalString());
    }

    static long subtractDates(String left, String right) {
        return parseDate(left).toEpochDay() - parseDate(right).toEpochDay();
    }

    static String subtractTimestamps(PostgresDataType temporalType, String left, String right) {
        switch (temporalType) {
        case TIMESTAMP:
            return between(parseTimestamp(left), parseTimestamp(right)).toCanonicalString();
        case TIMESTAMPTZ:
            return between(parseOffsetDateTime(left), parseOffsetDateTime(right)).toCanonicalString();
        default:
            throw new IgnoreMeException();
        }
    }

    static String addIntervals(String left, String right) {
        return parseInterval(left).plus(parseInterval(right)).toCanonicalString();
    }

    static String subtractIntervals(String left, String right) {
        return parseInterval(left).minus(parseInterval(right)).toCanonicalString();
    }

    static long extractField(TemporalField field, PostgresDataType sourceType, String value) {
        switch (sourceType) {
        case DATE:
            return extract(field, parseDate(value));
        case TIME:
            return extract(field, parseTime(value));
        case TIMETZ:
            return extract(field, parseOffsetTime(value));
        case TIMESTAMP:
            return extract(field, parseTimestamp(value));
        case TIMESTAMPTZ:
            return extract(field, parseOffsetDateTime(value));
        case INTERVAL:
            return extract(field, parseInterval(value));
        default:
            throw new IgnoreMeException();
        }
    }

    static String dateTrunc(TemporalField field, PostgresDataType sourceType, String value) {
        switch (sourceType) {
        case TIMESTAMP:
            return formatTimestamp(truncate(field, parseTimestamp(value)));
        case TIMESTAMPTZ:
            return formatOffsetTimestamp(truncate(field, parseOffsetDateTime(value)));
        default:
            throw new IgnoreMeException();
        }
    }

    static String justifyHours(String value) {
        IntervalValue interval = parseInterval(value);
        int extraDays = (int) (interval.getNanos() / nanosPerDay());
        long remainingNanos = interval.getNanos() % nanosPerDay();
        return new IntervalValue(interval.getMonths(), interval.getDays() + extraDays, remainingNanos).toCanonicalString();
    }

    static String justifyDays(String value) {
        IntervalValue interval = parseInterval(value);
        int extraMonths = interval.getDays() / 30;
        int remainingDays = interval.getDays() % 30;
        return new IntervalValue(interval.getMonths() + extraMonths, remainingDays, interval.getNanos())
                .toCanonicalString();
    }

    static String justifyInterval(String value) {
        return justifyDays(justifyHours(value));
    }

    static String makeInterval(int years, int months, int weeks, int days, int hours, int minutes, int seconds) {
        int totalMonths = years * 12 + months;
        int totalDays = weeks * 7 + days;
        long totalNanos = (((hours * 60L) + minutes) * 60L + seconds) * 1_000_000_000L;
        return new IntervalValue(totalMonths, totalDays, totalNanos).toCanonicalString();
    }

    static String timezone(String zone, String timestamptzValue) {
        ZoneOffset offset = parseZoneOffset(zone);
        return formatTimestamp(parseOffsetDateTime(timestamptzValue).atZoneSameInstant(offset).toLocalDateTime());
    }

    private static String normalizeOffset(String value) {
        String trimmed = value.trim();
        Matcher matcher = TRAILING_OFFSET_PATTERN.matcher(trimmed);
        if (!matcher.find()) {
            return trimmed;
        }
        String sign = matcher.group(1);
        String hour = matcher.group(2);
        String minute = matcher.group(3) == null ? "00" : matcher.group(3);
        return trimmed.substring(0, matcher.start()) + sign + hour + ":" + minute;
    }

    private static String normalizeTimestampSeparator(String value) {
        return value.trim().replace('T', ' ');
    }

    private static String formatDate(LocalDate date) {
        return date.toString();
    }

    private static String formatTime(LocalTime time) {
        return stripTrailingZeros(time.format(TIME_FORMATTER));
    }

    private static String formatOffsetTime(OffsetTime time) {
        return stripTrailingZeros(time.format(OFFSET_TIME_FORMATTER));
    }

    private static String formatTimestamp(LocalDateTime timestamp) {
        return stripTrailingZeros(timestamp.format(TIMESTAMP_FORMATTER));
    }

    private static String formatOffsetTimestamp(OffsetDateTime timestamp) {
        String text = timestamp.toLocalDateTime().format(TIMESTAMP_FORMATTER) + timestamp.getOffset().getId();
        return stripTrailingZeros(text.replace("Z", "+00:00"));
    }

    private static String stripTrailingZeros(String text) {
        if (!text.contains(".")) {
            return text;
        }
        int end = text.length();
        while (end > 0 && text.charAt(end - 1) == '0') {
            end--;
        }
        if (end > 0 && text.charAt(end - 1) == '.') {
            end--;
        }
        return text.substring(0, end);
    }

    private static String formatTimeNanos(long nanos) {
        long absoluteNanos = Math.abs(nanos);
        long totalSeconds = absoluteNanos / 1_000_000_000L;
        long nanoRemainder = absoluteNanos % 1_000_000_000L;
        long hours = totalSeconds / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;
        String formatted = String.format("%02d:%02d:%02d", hours, minutes, seconds);
        if (nanoRemainder != 0) {
            String fractional = String.format("%09d", nanoRemainder).replaceFirst("0+$", "");
            formatted += "." + fractional;
        }
        return nanos < 0 ? "-" + formatted : formatted;
    }

    private static long nanosPerDay() {
        return 24L * 60L * 60L * 1_000_000_000L;
    }

    private static IntervalValue negate(IntervalValue interval) {
        return new IntervalValue(-interval.getMonths(), -interval.getDays(), -interval.getNanos());
    }

    private static LocalDateTime applyInterval(LocalDateTime timestamp, IntervalValue interval) {
        return timestamp.plusMonths(interval.getMonths()).plusDays(interval.getDays()).plusNanos(interval.getNanos());
    }

    private static LocalTime applyInterval(LocalTime time, IntervalValue interval) {
        if (interval.getMonths() != 0) {
            throw new IgnoreMeException();
        }
        return time.plusNanos(interval.getDays() * nanosPerDay() + interval.getNanos());
    }

    private static OffsetTime applyInterval(OffsetTime time, IntervalValue interval) {
        if (interval.getMonths() != 0) {
            throw new IgnoreMeException();
        }
        return time.plusNanos(interval.getDays() * nanosPerDay() + interval.getNanos());
    }

    private static OffsetDateTime applyInterval(OffsetDateTime timestamp, IntervalValue interval) {
        return timestamp.plusMonths(interval.getMonths()).plusDays(interval.getDays()).plusNanos(interval.getNanos());
    }

    private static IntervalValue between(LocalDateTime left, LocalDateTime right) {
        long nanos = java.time.Duration.between(right, left).toNanos();
        return new IntervalValue(0, 0, nanos);
    }

    private static IntervalValue between(OffsetDateTime left, OffsetDateTime right) {
        long nanos = java.time.Duration.between(right.toInstant(), left.toInstant()).toNanos();
        return new IntervalValue(0, 0, nanos);
    }

    private static long extract(TemporalField field, LocalDate date) {
        switch (field) {
        case YEAR:
            return date.getYear();
        case MONTH:
            return date.getMonthValue();
        case DAY:
            return date.getDayOfMonth();
        default:
            throw new IgnoreMeException();
        }
    }

    private static long extract(TemporalField field, LocalTime time) {
        switch (field) {
        case HOUR:
            return time.getHour();
        case MINUTE:
            return time.getMinute();
        case SECOND:
            return time.getSecond();
        default:
            throw new IgnoreMeException();
        }
    }

    private static long extract(TemporalField field, OffsetTime time) {
        return extract(field, time.toLocalTime());
    }

    private static long extract(TemporalField field, LocalDateTime timestamp) {
        switch (field) {
        case YEAR:
            return timestamp.getYear();
        case MONTH:
            return timestamp.getMonthValue();
        case DAY:
            return timestamp.getDayOfMonth();
        case HOUR:
            return timestamp.getHour();
        case MINUTE:
            return timestamp.getMinute();
        case SECOND:
            return timestamp.getSecond();
        default:
            throw new IgnoreMeException();
        }
    }

    private static long extract(TemporalField field, OffsetDateTime timestamp) {
        return extract(field, timestamp.toLocalDateTime());
    }

    private static long extract(TemporalField field, IntervalValue interval) {
        switch (field) {
        case YEAR:
            return interval.getMonths() / 12;
        case MONTH:
            return interval.getMonths() % 12;
        case DAY:
            return interval.getDays();
        case HOUR:
            return Math.abs(interval.getNanos()) / 1_000_000_000L / 3600;
        case MINUTE:
            return (Math.abs(interval.getNanos()) / 1_000_000_000L % 3600) / 60;
        case SECOND:
            return Math.abs(interval.getNanos()) / 1_000_000_000L % 60;
        default:
            throw new IgnoreMeException();
        }
    }

    private static LocalDateTime truncate(TemporalField field, LocalDateTime timestamp) {
        switch (field) {
        case YEAR:
            return LocalDateTime.of(timestamp.getYear(), 1, 1, 0, 0);
        case MONTH:
            return LocalDateTime.of(timestamp.getYear(), timestamp.getMonthValue(), 1, 0, 0);
        case DAY:
            return LocalDateTime.of(timestamp.toLocalDate(), LocalTime.MIDNIGHT);
        case HOUR:
            return timestamp.withMinute(0).withSecond(0).withNano(0);
        case MINUTE:
            return timestamp.withSecond(0).withNano(0);
        case SECOND:
            return timestamp.withNano(0);
        default:
            throw new IgnoreMeException();
        }
    }

    private static OffsetDateTime truncate(TemporalField field, OffsetDateTime timestamp) {
        return truncate(field, timestamp.toLocalDateTime()).atOffset(timestamp.getOffset());
    }

    private static ZoneOffset parseZoneOffset(String zone) {
        String normalized = zone.trim().toUpperCase();
        if ("UTC".equals(normalized)) {
            return ZoneOffset.UTC;
        }
        try {
            return ZoneOffset.of(normalizeOffset(normalized));
        } catch (DateTimeParseException e) {
            throw new IgnoreMeException();
        }
    }
}
