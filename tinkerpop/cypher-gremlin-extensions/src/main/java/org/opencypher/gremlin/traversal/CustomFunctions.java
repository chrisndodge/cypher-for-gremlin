/*
 * Copyright (c) 2018-2019 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.gremlin.traversal;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

import org.joda.time.DateTime;
import org.joda.time.Period;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.opencypher.gremlin.translation.Tokens;
import org.opencypher.gremlin.translation.exception.ConstraintException;
import org.opencypher.gremlin.translation.exception.CypherExceptions;
import org.opencypher.gremlin.translation.exception.TypeException;

@SuppressWarnings({"unchecked", "WeakerAccess", "ArraysAsListWithZeroOrOneArgument"})
public final class CustomFunctions {
    private static SimpleDateFormat yearDateFormat = new SimpleDateFormat("YYYY");
    private static SimpleDateFormat monthDateFormat = new SimpleDateFormat("MM");
    private static SimpleDateFormat dayDateFormat = new SimpleDateFormat("dd");
    private static SimpleDateFormat dayOfYearDateFormat = new SimpleDateFormat("DD");
    private static SimpleDateFormat hourDateFormat = new SimpleDateFormat("HH");
    private static SimpleDateFormat minuteDateFormat = new SimpleDateFormat("mm");
    private static SimpleDateFormat secondDateFormat = new SimpleDateFormat("ss");
    private static SimpleDateFormat isoDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

    private CustomFunctions() {
    }

    public static Function<Traverser, Object> cypherToString() {
        return traverser -> {
            Object arg = tokenToNull(traverser.get());
            boolean valid = arg == null ||
                arg instanceof Boolean ||
                arg instanceof Number ||
                arg instanceof String;
            if (!valid) {
                String className = arg.getClass().getName();
                throw new TypeException("Cannot convert " + className + " to string");
            }

            return Optional.ofNullable(arg)
                .map(String::valueOf)
                .orElse(Tokens.NULL);
        };
    }

    public static Function<Traverser, Object> cypherToBoolean() {
        return traverser -> {
            Object arg = tokenToNull(traverser.get());
            boolean valid = arg == null ||
                arg instanceof Boolean ||
                arg instanceof String;
            if (!valid) {
                String className = arg.getClass().getName();
                throw new TypeException("Cannot convert " + className + " to boolean");
            }

            return Optional.ofNullable(arg)
                .map(String::valueOf)
                .map(v -> {
                    switch (v.toLowerCase()) {
                        case "true":
                            return true;
                        case "false":
                            return false;
                        default:
                            return Tokens.NULL;
                    }
                })
                .orElse(Tokens.NULL);
        };
    }

    public static Function<Traverser, Object> cypherToInteger() {
        return traverser -> {
            Object arg = tokenToNull(traverser.get());
            boolean valid = arg == null ||
                arg instanceof Number ||
                arg instanceof String;
            if (!valid) {
                String className = arg.getClass().getName();
                throw new TypeException("Cannot convert " + className + " to integer");
            }

            return nullToToken(
                Optional.ofNullable(arg)
                    .map(String::valueOf)
                    .map(v -> {
                        try {
                            return Long.valueOf(v);
                        } catch (NumberFormatException e1) {
                            try {
                                return Double.valueOf(v).longValue();
                            } catch (NumberFormatException e2) {
                                return null;
                            }
                        }
                    })
                    .orElse(null));
        };
    }

    public static Function<Traverser, Object> cypherToFloat() {
        return traverser -> {
            Object arg = tokenToNull(traverser.get());
            boolean valid = arg == null ||
                arg instanceof Number ||
                arg instanceof String;
            if (!valid) {
                String className = arg.getClass().getName();
                throw new TypeException("Cannot convert " + className + " to float");
            }

            return nullToToken(
                Optional.ofNullable(arg)
                    .map(String::valueOf)
                    .map(v -> {
                        try {
                            return Double.valueOf(v);
                        } catch (NumberFormatException e) {
                            return null;
                        }
                    })
                    .orElse(null));
        };
    }

    public static Function<Traverser,Object> cypherRound() {
        return cypherFunction(a -> (Math.round((Double) a.get(0))), Double.class);
    }

    public static Function<Traverser, Object> cypherProperties() {
        return traverser -> {
            Object argument = traverser.get();

            if (argument == Tokens.NULL) {
                return Tokens.NULL;
            }

            if (argument instanceof Map) {
                return argument;
            }
            Iterator<? extends Property<Object>> it = ((Element) argument).properties();
            Map<Object, Object> propertyMap = new HashMap<>();
            while (it.hasNext()) {
                Property<Object> property = it.next();
                propertyMap.putIfAbsent(property.key(), property.value());
            }
            return propertyMap;
        };
    }

    public static Function<Traverser, Object> cypherContainerIndex() {
        return traverser -> {
            List<?> args = (List<?>) traverser.get();
            Object container = args.get(0);
            Object index = args.get(1);

            if (container == Tokens.NULL || index == Tokens.NULL) {
                return Tokens.NULL;
            }

            if (container instanceof List) {
                List list = (List) container;
                int size = list.size();
                int i = normalizeContainerIndex(index, size);
                if (i < 0 || i > size) {
                    return Tokens.NULL;
                }
                return list.get(i);
            }

            if (container instanceof Map) {
                if (!(index instanceof String)) {
                    String indexClass = index.getClass().getName();
                    throw new IllegalArgumentException("Map element access by non-string: " + indexClass);
                }
                Map map = (Map) container;
                String key = (String) index;
                return map.getOrDefault(key, Tokens.NULL);
            }

            if (container instanceof Element) {
                if (!(index instanceof String)) {
                    String indexClass = index.getClass().getName();
                    throw new IllegalArgumentException("Property access by non-string: " + indexClass);
                }
                Element element = (Element) container;
                String key = (String) index;
                return element.property(key).orElse(Tokens.NULL);
            }

            /*
            if (container instanceof DateTime) {
                if (!(index instanceof String)) {
                    String indexClass = index.getClass().getName();
                    throw new IllegalArgumentException("Property access by non-string: " + indexClass);
                }
                String fieldName = (String) index;
                try {
                    Field field = container.getClass().getDeclaredField(fieldName);
                    field.setAccessible(true);
                    Object value = field.get(container);
                    return (value != null) ? value : Tokens.NULL;

                } catch(NoSuchFieldException | IllegalAccessException ex) {
                    throw new IllegalArgumentException("Invalid property access of " + container.getClass().getName());
                }
            }
            */

            String containerClass = container.getClass().getName();
            if (index instanceof String) {
                throw new IllegalArgumentException("Invalid property access of " + containerClass);
            }
            throw new IllegalArgumentException("Invalid element access of " + containerClass);
        };
    }

    public static Function<Traverser, Object> cypherListSlice() {
        return traverser -> {
            List<?> args = (List<?>) traverser.get();
            Object container = args.get(0);
            Object from = args.get(1);
            Object to = args.get(2);

            if (container == Tokens.NULL || from == Tokens.NULL || to == Tokens.NULL) {
                return Tokens.NULL;
            }

            if (container instanceof List) {
                List list = (List) container;
                int size = list.size();
                int f = normalizeRangeIndex(from, size);
                int t = normalizeRangeIndex(to, size);
                if (f >= t) {
                    return new ArrayList<>();
                }
                return new ArrayList<>(list.subList(f, t));
            }

            String containerClass = container.getClass().getName();
            throw new IllegalArgumentException(
                "Invalid element access of " + containerClass + " by range"
            );
        };
    }

    private static int normalizeContainerIndex(Object index, int containerSize) {
        if (!(index instanceof Number)) {
            String indexClass = index.getClass().getName();
            throw new IllegalArgumentException("List element access by non-integer: " + indexClass);
        }
        int i = ((Number) index).intValue();
        return (i >= 0) ? i : containerSize + i;
    }

    private static int normalizeRangeIndex(Object index, int size) {
        int i = normalizeContainerIndex(index, size);
        if (i < 0) {
            return 0;
        }
        if (i > size) {
            return size;
        }
        return i;
    }

    public static Function<Traverser, Object> cypherPercentileCont() {
        return percentileFunction(
            (data, percentile) -> {
                int last = data.size() - 1;
                double lowPercentile = Math.floor(percentile * last) / last;
                double highPercentile = Math.ceil(percentile * last) / last;
                if (lowPercentile == highPercentile) {
                    return percentileNearest(data, percentile);
                }

                double scale = (percentile - lowPercentile) / (highPercentile - lowPercentile);
                double low = percentileNearest(data, lowPercentile).doubleValue();
                double high = percentileNearest(data, highPercentile).doubleValue();
                return (high - low) * scale + low;
            }
        );
    }

    public static Function<Traverser, Object> cypherPercentileDisc() {
        return percentileFunction(
            CustomFunctions::percentileNearest
        );
    }

    private static Function<Traverser, Object> percentileFunction(BiFunction<List<Number>, Double, Number> percentileStrategy) {
        return traverser -> {
            List<?> args = (List<?>) traverser.get();

            double percentile = ((Number) args.get(1)).doubleValue();
            if (percentile < 0 || percentile > 1) {
                throw new IllegalArgumentException("Number out of range: " + percentile);
            }

            Collection<?> coll = (Collection<?>) args.get(0);
            boolean invalid = coll.stream()
                .anyMatch(o -> !(o == null || o instanceof Number));
            if (invalid) {
                throw new IllegalArgumentException("Percentile function can only handle numerical values");
            }
            List<Number> data = coll.stream()
                .filter(Objects::nonNull)
                .map(o -> (Number) o)
                .sorted()
                .collect(toList());

            int size = data.size();
            if (size == 0) {
                return Tokens.NULL;
            } else if (size == 1) {
                return data.get(0);
            }

            return percentileStrategy.apply(data, percentile);
        };
    }

    private static <T> T percentileNearest(List<T> sorted, double percentile) {
        int size = sorted.size();
        int index = (int) Math.ceil(percentile * size) - 1;
        if (index == -1) {
            index = 0;
        }
        return sorted.get(index);
    }

    public static Function<Traverser, Object> cypherSize() {
        return traverser -> traverser.get() instanceof String ?
            (long) ((String) traverser.get()).length() :
            (long) ((Collection) traverser.get()).size();
    }

    public static Function<Traverser, Object> cypherPlus() {
        return traverser -> {
            List<?> args = (List<?>) traverser.get();
            Object a = args.get(0);
            Object b = args.get(1);

            if (a == Tokens.NULL || b == Tokens.NULL) {
                return Tokens.NULL;
            }

            if (a instanceof List || b instanceof List) {
                List<Object> objects = new ArrayList<>();
                if (a instanceof List) {
                    objects.addAll((List<?>) a);
                } else {
                    objects.add(a);
                }
                if (b instanceof List) {
                    objects.addAll((List<?>) b);
                } else {
                    objects.add(b);
                }
                return objects;
            }

            if (a instanceof Date && b instanceof Duration) {
                return Date.from(((Date)a).toInstant().plus(((Duration)b).toMillis()/1000, ChronoUnit.SECONDS));
            }

            if (a instanceof Date && b instanceof Period) {
                DateTime d = new DateTime((Date)a);
                return d.plus((Period) b).toDate();
            }

            if (!(a instanceof String || a instanceof Number) ||
                !(b instanceof String || b instanceof Number)) {
                throw new TypeException("Illegal use of plus operator");
            }

            if (a instanceof Number && b instanceof Number) {
                if (a instanceof Double || b instanceof Double ||
                    a instanceof Float || b instanceof Float) {
                    return ((Number) a).doubleValue() + ((Number) b).doubleValue();
                } else {
                    return ((Number) a).longValue() + ((Number) b).longValue();
                }
            } else {
                return String.valueOf(a) + String.valueOf(b);
            }
        };
    }

    public static Function<Traverser, Object> cypherMinus() {
        return traverser -> {
            List<?> args = (List<?>) traverser.get();
            Object a = args.get(0);
            Object b = args.get(1);

            // right now we only extend the minus operator to cover date - duration types
            if (a instanceof Date && b instanceof Duration) {
                return Date.from(((Date)a).toInstant().minus(((Duration)b).toMillis()/1000, ChronoUnit.SECONDS));
            }

            if (a instanceof Date && b instanceof Period) {
                DateTime d = new DateTime((Date)a);
                return d.minus((Period) b).toDate();
            }

            if (!(a instanceof String || a instanceof Number) ||
                !(b instanceof String || b instanceof Number)) {
                throw new TypeException("Illegal use of minus operator");
            }

            if (a instanceof Number && b instanceof Number) {
                if (a instanceof Double || b instanceof Double ||
                    a instanceof Float || b instanceof Float) {
                    return ((Number) a).doubleValue() - ((Number) b).doubleValue();
                } else {
                    return ((Number) a).longValue() - ((Number) b).longValue();
                }
            }

            throw new TypeException("Illegal use of minus operator");
        };
    }

    public static Function<Traverser, Object> cypherReverse() {
        return traverser -> {
            Object o = traverser.get();
            if (o == Tokens.NULL) {
                return Tokens.NULL;
            } else if (o instanceof Collection) {
                ArrayList result = new ArrayList((Collection) o);
                Collections.reverse(result);
                return result;
            } else if (o instanceof String) {
                return new StringBuilder((String) o).reverse().toString();
            } else {
                throw new TypeException(format("Expected a string or list value for reverse, but got: %s(%s)",
                    o.getClass().getSimpleName(), o));
            }
        };
    }

    public static Function<Traverser, Object> cypherSubstring() {
        return traverser -> {
            List<?> args = (List<?>) traverser.get();
            Object a = args.get(0);
            Object b = args.get(1);

            if (a == Tokens.NULL) {
                return Tokens.NULL;
            } else if (!(a instanceof String) || (!(b instanceof Number))) {
                throw new TypeException(format("Expected substring(String, Integer, [Integer]), but got: (%s, %s)",
                    a, b));
            } else if (args.size() == 3 && (!(args.get(2) instanceof Number))) {
                throw new TypeException(format("Expected substring(String, Integer, [Integer]), but got: (%s, %s, %s)",
                    a, b, args.get(2)));
            } else if (args.size() == 3) {
                String s = (String) a;
                int endIndex = ((Number) b).intValue() + ((Number) args.get(2)).intValue();
                endIndex = endIndex > s.length() ? s.length() : endIndex;
                return s.substring(((Number) b).intValue(), endIndex);
            } else {
                return ((String) a).substring(((Number) b).intValue());
            }
        };
    }

    private static Function<Traverser, Object> cypherFunction(Function<List, Object> func, Class<?>... clazzes) {
        return traverser -> {
            List args = traverser.get() instanceof List ? ((List) traverser.get()) : asList(traverser.get());

            for (int i = 0; i < clazzes.length; i++) {
                if (args.get(i) == Tokens.NULL) {
                    return Tokens.NULL;
                }

                if (!clazzes[i].isInstance(args.get(i))) {
                    throw new TypeException(format("Expected a %s value for <function1>, but got: %s(%s)",
                        clazzes[i].getSimpleName(),
                        args.get(i).getClass().getSimpleName(),
                        args.get(i)));
                }
            }

            return func.apply(args);
        };
    }

    public static Function<Traverser,Object> cypherTrim() {
        return cypherFunction(a -> ((String) a.get(0)).trim(), String.class);
    }

    public static Function<Traverser, Object> cypherToUpper() {
        return cypherFunction(a -> ((String) a.get(0)).toUpperCase(), String.class);
    }

    public static Function<Traverser, Object> cypherToLower() {
        return cypherFunction(a -> ((String) a.get(0)).toLowerCase(), String.class);
    }

    public static Function<Traverser, Object> cypherSplit() {
        return cypherFunction(a -> asList(((String) a.get(0)).split((String) a.get(1))), String.class, String.class);
    }

    public static Function<Traverser, Object> cypherReplace() {
        return cypherFunction(a ->
                ((String) a.get(0)).replace((String) a.get(1), (String) a.get(2)),
            String.class, String.class, String.class);
    }

    public static Function<Traverser, Object> cypherException() {
        return traverser -> {
            String message = CypherExceptions.messageByName(traverser.get());
            throw new ConstraintException(message);
        };
    }

    private static Object tokenToNull(Object maybeNull) {
        return Tokens.NULL.equals(maybeNull) ? null : maybeNull;
    }

    private static Object nullToToken(Object maybeNull) {
        return maybeNull == null ? Tokens.NULL : maybeNull;
    }

    private static <T> T cast(Object o, Class<T> clazz) {
        if (clazz.isInstance(o)) {
            return clazz.cast(o);
        } else {
            throw new TypeException(format("Expected %s to be %s, but it was %s",
                o, clazz.getSimpleName(), o.getClass().getSimpleName()));
        }
    }

    /*
    *
    * SAMOS SPECIFIC DATE/TIME CUSTOM FUNCTIONS
    *
    */

    public static Function<Traverser,Object> cypherUtcNow() {
        return traverser -> {
            return Date.from(Instant.now());
        };
    }

    /*
    public static Function<Traverser,Object> cypherUtcNow() {
        return traverser -> {
            return new DateTime(DateTimeZone.UTC);
        };
    }
    */

    private static <K, V> V getOrDefault(Map<K,V> map, K key, V defaultValue) {
        return map.containsKey(key) ? map.get(key) : defaultValue;
    }

    public static Function<Traverser,Object> cypherDuration() {
        return traverser -> {
            Object arg = traverser.get();
            String isoDuration = "";
            if (arg instanceof String) {
                isoDuration = (String)arg;
            } else if (arg instanceof Map<?, ?>) {
                Map<String, Double> map = (Map<String, Double>) arg;
               
                Double years = map.containsKey("years") ? map.get("years") : 0.0D;
                Double quarters = map.containsKey("quarters") ? map.get("quarters") : 0.0D;
                Double months = map.containsKey("months") ? map.get("months") : 0.0D; 
                Double weeks = map.containsKey("weeks") ? map.get("weeks") : 0.0D;
                Double days = map.containsKey("days") ? map.get("days") : 0.0D;
                Double hours = map.containsKey("hours") ? map.get("hours") : 0.0D;
                Double minutes = map.containsKey("minutes") ? map.get("minutes") : 0.0D;
                Double seconds = map.containsKey("seconds") ? map.get("seconds") : 0.0D;
                Double milliseconds = map.containsKey("milliseconds") ? map.get("milliseconds") : 0.0D;
                Double microseconds = map.containsKey("microseconds") ? map.get("microseconds") : 0.0D;
                Double nanoseconds = map.containsKey("nanoseconds") ? map.get("nanoseconds") : 0.0D;

                quarters += years * 4.0D;
                months += quarters * 3.0D;

                // 30.4375 days in a month tries to account for the average considering different days in month:
                // >>> (31 + 28.25 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31) / 12
                //      30.4375
                days += months * 30.4375D;
                days += weeks * 7.0D;
                hours += days * 24.0D;
                minutes += hours * 60.0D;
                seconds += minutes * 60.0D;

                seconds += (milliseconds.doubleValue() / 1000.0D) + 
                    (microseconds.doubleValue() / (1000.0D * 1000.0D)) + 
                    (nanoseconds.doubleValue() / (1000.0D * 1000.0D * 1000.0D));

                isoDuration = String.format("PT%.9fS", seconds);
            }

            return Duration.parse(isoDuration);
        };
    }

    public static Function<Traverser, Object> cypherPeriod() {
        return traverser -> {
            Object arg = traverser.get();
            String isoDuration = "";
            if (arg instanceof String) {
                return Period.parse((String) arg);
            } else if (arg instanceof Map<?, ?>) {
                Map<String, Long> map = (Map<String, Long>) arg;
               
                Long years = map.containsKey("years") ? map.get("years") : 0L;
                Long months = map.containsKey("months") ? map.get("months") : 0L; 
                Long weeks = map.containsKey("weeks") ? map.get("weeks") : 0L;
                Long days = map.containsKey("days") ? map.get("days") : 0L;
                Long hours = map.containsKey("hours") ? map.get("hours") : 0L;
                Long minutes = map.containsKey("minutes") ? map.get("minutes") : 0L;
                Long seconds = map.containsKey("seconds") ? map.get("seconds") : 0L;
                Long milliseconds = map.containsKey("milliseconds") ? map.get("milliseconds") : 0L;

                return new Period(
                    years.intValue(), months.intValue(), weeks.intValue(), days.intValue(),
                    hours.intValue(), minutes.intValue(), seconds.intValue(), milliseconds.intValue()
                );
            }

            throw new TypeException("DURATION() must be passed in a string or map type");            
        };
    }

    public static Function<Traverser,Object> cypherDateAdd() {
        return traverser -> {
            List<?> args = (List<?>) traverser.get();
            if (args.size() != 7) {
                throw new TypeException("Incorrect number of arguments. Usage: dateadd(date, years, months, days, hours, minutes, seconds)");
            }
            Date theDate = (Date) args.get(0);
            Long years = (Long) args.get(1);
            Long months = (Long) args.get(2);
            Long days = (Long) args.get(3);
            Long hours = (Long) args.get(4);
            Long minutes = (Long) args.get(5);
            Long seconds = (Long) args.get(6);

            Calendar c = Calendar.getInstance();
            c.setTime(theDate);

            // Perform addition/subtraction
            c.add(Calendar.YEAR, years.intValue());
            c.add(Calendar.MONTH, months.intValue());
            c.add(Calendar.DATE, days.intValue());
            c.add(Calendar.HOUR, hours.intValue());
            c.add(Calendar.MINUTE, minutes.intValue());
            c.add(Calendar.SECOND, seconds.intValue());

            // Convert calendar back to Date
            return c.getTime();
        };
    }

    
    /*public static Function<Traverser,Object> cypherYear() {
        return cypherFunction(a -> {
            LocalDate localDate = ((Date) a).toInstant().atZone(ZoneId.of("UTC")).toLocalDate();
            return localDate.getYear();
        }, Date.class);
    }*/

    public static Function<Traverser,Object> cypherYear() {
        return cypherFunction(
            a -> CustomFunctions.yearDateFormat.format((Date) a.get(0)), 
            Date.class
        );
    }

    public static Function<Traverser,Object> cypherMonth() {
        return cypherFunction(
            a -> CustomFunctions.monthDateFormat.format((Date) a.get(0)), 
            Date.class
        );
    }

    public static Function<Traverser,Object> cypherDay() {
        return cypherFunction(
            a -> CustomFunctions.dayDateFormat.format((Date) a.get(0)), 
            Date.class
        );
    }

    public static Function<Traverser,Object> cypherDayOfYear() {
        return cypherFunction(
            a -> CustomFunctions.dayOfYearDateFormat.format((Date) a.get(0)), 
            Date.class
        );
    }

    public static Function<Traverser,Object> cypherHour() {
        return cypherFunction(
            a -> CustomFunctions.hourDateFormat.format((Date) a.get(0)), 
            Date.class
        );
    }

    public static Function<Traverser,Object> cypherMinute() {
        return cypherFunction(
            a -> CustomFunctions.minuteDateFormat.format((Date) a.get(0)), 
            Date.class
        );
    }

    public static Function<Traverser,Object> cypherSecond() {
        return cypherFunction(
            a -> CustomFunctions.secondDateFormat.format((Date) a.get(0)), 
            Date.class
        );
    }

    public static Function<Traverser,Object> cypherDate() {
        return traverser -> {
            List<?> args = (List<?>) traverser.get();
            if (args.size() != 1) {
                throw new TypeException("Incorrect number of arguments. Usage: DATE(string | map)");
            }            

            Object arg = args.get(0);
            if (arg instanceof Map<?, ?>) {
                Map<String, Long> map = (Map<String, Long>) arg;
                Calendar c = Calendar.getInstance();

                Integer year = map.containsKey("year") ? ((Long)map.get("year")).intValue() : 1;

                Integer week = map.containsKey("week") ? ((Long)map.get("week")).intValue() : 0;
                Integer dayOfWeek = map.containsKey("dayOfWeek") ? ((Long)map.get("dayOfWeek")).intValue() : 0;

                if (week > 0) {
                    c.setWeekDate(year, week, dayOfWeek);
                    return c.getTime();
                }

                Integer month = map.containsKey("month") ? ((Long)map.get("month")).intValue() : 1; 
                Integer day = map.containsKey("day") ? ((Long)map.get("day")).intValue() : 1;
                Integer hour = map.containsKey("hour") ? ((Long)map.get("hour")).intValue() : 0;
                Integer minute = map.containsKey("minute") ? ((Long)map.get("minute")).intValue() : 0;
                Integer second = map.containsKey("second") ? ((Long)map.get("second")).intValue() : 0;
                Integer millisecond = map.containsKey("millisecond") ? ((Long)map.get("millisecond")).intValue() : 0;

                c.set(year, month-1, day, hour, minute, second); // the library month starts at 0, but user expects start at 1
                c.set(Calendar.MILLISECOND, millisecond);

                return c.getTime();
            } else if (arg instanceof String) {
                return DateTime.parse((String) arg).toDate();
            }

            throw new TypeException("DATE() must be passed in a string or map type");
        };
    }

    public static Function<Traverser,Object> cypherTruncateDate() {
        return traverser -> {
            List<?> args = (List<?>) traverser.get();
            if (args.size() != 2) {
                throw new TypeException("Incorrect number of arguments. Usage: TRUNCATE_DATE(date, resolution)");
            }
            Date inDate = (Date) args.get(0);
            String resolution = ((String) args.get(1)).toLowerCase();

            Calendar cal = Calendar.getInstance();
            cal.setTime(inDate);
            cal.set(Calendar.MILLISECOND, 0); // always to ms at least

            if (resolution.equals("second")) {
                return cal.getTime();
            }

            cal.set(Calendar.SECOND, 0);
            if (resolution.equals("minute")) {
                return cal.getTime();
            }

            cal.set(Calendar.MINUTE, 0);
            if (resolution.equals("hour")) {
                return cal.getTime();
            }

            cal.set(Calendar.HOUR_OF_DAY, 0);
            if (resolution.equals("day")) {
                return cal.getTime();
            }

            cal.set(Calendar.DAY_OF_MONTH, 1);
            if (resolution.equals("month")) {
                return cal.getTime();
            }

            cal.set(Calendar.MONTH, 0);
            if (resolution.equals("year")) {
                return cal.getTime();
            }

            throw new TypeException("TRUNCATE_DATE resolution must be one of 'second', 'minute', 'hour', 'day', 'month', 'year'");
        };
    }

}
