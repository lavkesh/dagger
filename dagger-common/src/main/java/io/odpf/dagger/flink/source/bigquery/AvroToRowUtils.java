package io.odpf.dagger.flink.source.bigquery;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class AvroToRowUtils {
    private static final JodaConvertor jodaConverter = JodaConvertor.getConverter();
    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    private static final long MICROS_PER_SECOND = 1_000_000L;

    public static Row convertAvroRecordToRow(Schema schema, RowTypeInfo typeInfo, IndexedRecord record, int extraFields) {
        final List<Schema.Field> fields = schema.getFields();
        final TypeInformation<?>[] fieldInfo = typeInfo.getFieldTypes();
        final int length = fields.size();
        final Row row = new Row(length + extraFields);
        for (int i = 0; i < length; i++) {
            final Schema.Field field = fields.get(i);
            row.setField(i, convertAvroType(field.schema(), fieldInfo[i], record.get(i)));
        }
        return row;
    }

    private static Object convertAvroType(Schema schema, TypeInformation<?> info, Object object) {
        // we perform the conversion based on schema information but enriched with pre-computed
        // type information where useful (i.e., for arrays)

        if (object == null) {
            return null;
        }
//        System.out.println("TYPE:" + schema.getType() + " info:" + info + " Object: " + object);
        switch (schema.getType()) {
            case RECORD:
                if (object instanceof IndexedRecord) {
                    return convertAvroRecordToRow(
                            schema, (RowTypeInfo) info, (IndexedRecord) object, 0);
                }
                throw new IllegalStateException(
                        "IndexedRecord expected but was: " + object.getClass());
            case ENUM:
            case STRING:
                return object.toString();
            case ARRAY:
                if (info instanceof BasicArrayTypeInfo) {
                    final TypeInformation<?> elementInfo =
                            ((BasicArrayTypeInfo<?, ?>) info).getComponentInfo();
                    return convertToObjectArray(schema.getElementType(), elementInfo, object);
                } else {
                    final TypeInformation<?> elementInfo =
                            ((ObjectArrayTypeInfo<?, ?>) info).getComponentInfo();
                    return convertToObjectArray(schema.getElementType(), elementInfo, object);
                }
            case MAP:
                final MapTypeInfo<?, ?> mapTypeInfo = (MapTypeInfo<?, ?>) info;
                final Map<String, Object> convertedMap = new HashMap<>();
                final Map<?, ?> map = (Map<?, ?>) object;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    convertedMap.put(
                            entry.getKey().toString(),
                            convertAvroType(
                                    schema.getValueType(),
                                    mapTypeInfo.getValueTypeInfo(),
                                    entry.getValue()));
                }
                return convertedMap;
            case UNION:
                final List<Schema> types = schema.getTypes();
                final int size = types.size();
                if (size == 2 && types.get(0).getType() == Schema.Type.NULL) {
                    return convertAvroType(types.get(1), info, object);
                } else if (size == 2 && types.get(1).getType() == Schema.Type.NULL) {
                    return convertAvroType(types.get(0), info, object);
                } else if (size == 1) {
                    return convertAvroType(types.get(0), info, object);
                } else {
                    return object;
                }
            case FIXED:
                final byte[] fixedBytes = ((GenericFixed) object).bytes();
                if (info == Types.BIG_DEC) {
                    return convertToDecimal(schema, fixedBytes);
                }
                return fixedBytes;
            case BYTES:
                final ByteBuffer byteBuffer = (ByteBuffer) object;
                final byte[] bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
                if (info == Types.BIG_DEC) {
                    return convertToDecimal(schema, bytes);
                }
                return bytes;
            case INT:
                if (info == Types.SQL_DATE) {
                    return convertToDate(object);
                } else if (info == Types.SQL_TIME) {
                    return convertToTime(object);
                }
                return object;
            case LONG:
                if (info == Types.SQL_TIMESTAMP) {
                    return convertToTimestamp(
                            object, schema.getLogicalType() == LogicalTypes.timestampMicros());
                } else if (info == Types.SQL_TIME) {
                    return convertToTime(object);
                }
                return object;
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                return object;
        }
        throw new RuntimeException("Unsupported Avro type:" + schema);
    }

    private static BigDecimal convertToDecimal(Schema schema, byte[] bytes) {
        final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
        return new BigDecimal(new BigInteger(bytes), decimalType.getScale());
    }

    private static Date convertToDate(Object object) {
        final long millis;
        if (object instanceof Integer) {
            final Integer value = (Integer) object;
            // adopted from Apache Calcite
            final long t = (long) value * 86400000L;
            millis = t - (long) LOCAL_TZ.getOffset(t);
        } else if (object instanceof LocalDate) {
            long t = ((LocalDate) object).toEpochDay() * 86400000L;
            millis = t - (long) LOCAL_TZ.getOffset(t);
        } else if (jodaConverter != null) {
            millis = jodaConverter.convertDate(object);
        } else {
            throw new IllegalArgumentException(
                    "Unexpected object type for DATE logical type. Received: " + object);
        }
        return new Date(millis);
    }

    private static Time convertToTime(Object object) {
        final long millis;
        if (object instanceof Integer) {
            millis = (Integer) object;
        } else if (object instanceof Long) {
            millis = (Long) object / 1000L;
        } else if (object instanceof LocalTime) {
            millis = ((LocalTime) object).get(ChronoField.MILLI_OF_DAY);
        } else if (jodaConverter != null) {
            millis = jodaConverter.convertTime(object);
        } else {
            throw new IllegalArgumentException(
                    "Unexpected object type for DATE logical type. Received: " + object);
        }
        return new Time(millis - LOCAL_TZ.getOffset(millis));
    }

    private static Timestamp convertToTimestamp(Object object, boolean isMicros) {
        final long millis;
        if (object instanceof Long) {
            if (isMicros) {
                long micros = (Long) object;
                int offsetMillis = LOCAL_TZ.getOffset(micros / 1000L);

                long seconds = micros / MICROS_PER_SECOND - offsetMillis / 1000;
                int nanos =
                        ((int) (micros % MICROS_PER_SECOND)) * 1000 - offsetMillis % 1000 * 1000;
                Timestamp timestamp = new Timestamp(seconds * 1000L);
                timestamp.setNanos(nanos);
                return timestamp;
            } else {
                millis = (Long) object;
            }
        } else if (object instanceof Instant) {
            Instant instant = (Instant) object;
            int offsetMillis = LOCAL_TZ.getOffset(instant.toEpochMilli());

            long seconds = instant.getEpochSecond() - offsetMillis / 1000;
            int nanos = instant.getNano() - offsetMillis % 1000 * 1000;
            Timestamp timestamp = new Timestamp(seconds * 1000L);
            timestamp.setNanos(nanos);
            return timestamp;
        } else if (jodaConverter != null) {
            millis = jodaConverter.convertTimestamp(object);
        } else {
            throw new IllegalArgumentException(
                    "Unexpected object type for DATE logical type. Received: " + object);
        }
        return new Timestamp(millis - LOCAL_TZ.getOffset(millis));
    }

    private static Object[] convertToObjectArray(
            Schema elementSchema, TypeInformation<?> elementInfo, Object object) {
        final List<?> list = (List<?>) object;
        final Object[] convertedArray =
                (Object[]) Array.newInstance(elementInfo.getTypeClass(), list.size());
        for (int i = 0; i < list.size(); i++) {
            convertedArray[i] = convertAvroType(elementSchema, elementInfo, list.get(i));
        }
        return convertedArray;
    }

}
