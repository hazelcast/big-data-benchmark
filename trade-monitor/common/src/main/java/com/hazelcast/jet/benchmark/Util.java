package com.hazelcast.jet.benchmark;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public final class Util {
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
            return Integer.parseInt(prop.replace("_", ""));
        } catch (NumberFormatException e) {
            throw new ValidationException("Invalid property format, correct example is 9_999: " + propName + "=" + prop);
        }
    }

}
