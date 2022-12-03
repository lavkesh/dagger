package io.odpf.dagger.flink.source.bigquery;


import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

/**
 * Encapsulates joda optional dependency. Instantiates this class only if joda is available on the
 * classpath.
 */
class JodaConvertor {

    private static JodaConvertor instance;
    private static boolean instantiated = false;

    public static JodaConvertor getConverter() {
        if (instantiated) {
            return instance;
        }

        try {
            Class.forName(
                    "org.joda.time.DateTime",
                    false,
                    Thread.currentThread().getContextClassLoader());
            instance = new JodaConvertor();
        } catch (ClassNotFoundException e) {
            instance = null;
        } finally {
            instantiated = true;
        }
        return instance;
    }

    public long convertDate(Object object) {
        final LocalDate value = (LocalDate) object;
        return value.toDate().getTime();
    }

    public int convertTime(Object object) {
        final LocalTime value = (LocalTime) object;
        return value.get(DateTimeFieldType.millisOfDay());
    }

    public long convertTimestamp(Object object) {
        final DateTime value = (DateTime) object;
        return value.toDate().getTime();
    }

    private JodaConvertor() {}
}

