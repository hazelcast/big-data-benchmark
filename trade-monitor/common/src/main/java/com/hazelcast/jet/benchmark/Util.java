package com.hazelcast.jet.benchmark;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import static com.hazelcast.jet.benchmark.Trade.TICKER_MAX_LENGTH;

public final class Util {
    public static final String KAFKA_TOPIC = "trades";
    public static final int TRADE_BLOB_SIZE = TICKER_MAX_LENGTH + Long.BYTES + Integer.BYTES + Integer.BYTES;
    private static final byte PADDING_BYTE = (byte) ' ';

    private Util() {
    }

    public static Properties props(String... namesAndValues) {
        Properties props = new Properties();
        for (int i = 0; i < namesAndValues.length; i += 2) {
            props.setProperty(namesAndValues[i], namesAndValues[i + 1]);
        }
        return props;
    }

    public static Properties loadProps(String propsPath) {
        String resolvedPropsPath = new File(propsPath).getAbsolutePath();
        System.out.println("Configuration file: " + resolvedPropsPath);
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(propsPath));
        } catch (FileNotFoundException e) {
            System.err.println("File not found: " + e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Can't read file " + propsPath);
            System.exit(2);
        }
        return props;
    }

    public static void printParams(Object... namesAndValues) {
        for (int i = 0; i < namesAndValues.length; i += 2) {
            String name = (String) namesAndValues[i];
            Object value = namesAndValues[i + 1];
            if (value instanceof Integer) {
                value = String.format("%,d", value);
            }
            System.out.println("    " + name + "=" + value);
        }
    }

    public  static String ensureProp(Properties props, String propName) throws ValidationException {
        String prop = props.getProperty(propName);
        if (prop == null || prop.isEmpty()) {
            throw new ValidationException("Missing property: " + propName);
        }
        return prop;
    }

    public static int parseIntProp(Properties props, String propName) throws ValidationException {
        String prop = ensureProp(props, propName);
        try {
            int value = Integer.parseInt(prop.replace("_", ""));
            if (value <= 0) {
                throw new ValidationException("Value must not be negative: " + propName + "=" + prop);
            }
            return value;
        } catch (NumberFormatException e) {
            throw new ValidationException("Invalid property format, correct example is 9_999: " + propName + "=" + prop);
        }
    }

    public static boolean parseBooleanProp(Properties props, String propName) throws ValidationException {
        String prop = ensureProp(props, propName);
        try {
            return Boolean.parseBoolean(prop);
        } catch (NumberFormatException e) {
            throw new ValidationException("Invalid property format, expected true or false: " + propName + "=" + prop);
        }
    }

    public static byte[] serializeTrade(Trade trade) {
        ByteBuffer buf = ByteBuffer.allocate(TRADE_BLOB_SIZE);
        String ticker = trade.getTicker();
        if (ticker.length() > TICKER_MAX_LENGTH) {
            throw new RuntimeException("This ticker is too long: " + ticker + ". Max length is " + TICKER_MAX_LENGTH);
        }
        int i = 0;
        for (; i < ticker.length(); i++) {
            buf.put((byte) ticker.charAt(i));
        }
        for (; i < TICKER_MAX_LENGTH; i++) {
            buf.put(PADDING_BYTE);
        }
        buf.putLong(trade.getTime());
        buf.putInt(trade.getPrice());
        buf.putInt(trade.getQuantity());
        return buf.array();
    }

    public static Trade deserializeTrade(byte[] bytes) {
        int tickerLen = 0;
        while (bytes[tickerLen] != PADDING_BYTE && tickerLen < TICKER_MAX_LENGTH) {
            tickerLen++;
        }
        String ticker = new String(bytes, 0, tickerLen);
        ByteBuffer buf = ByteBuffer.wrap(bytes, TICKER_MAX_LENGTH, TRADE_BLOB_SIZE - TICKER_MAX_LENGTH);
        long time = buf.getLong();
        int price = buf.getInt();
        int quantity = buf.getInt();
        return new Trade(time, ticker, quantity, price);
    }
}
