package org.jocean.redis;

import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ThreadFactoryUtil {
    private ThreadFactoryUtil() {
        throw new IllegalStateException("No instances!");
    }

    public static ThreadFactory build(final String namePrefix) {
        return new ThreadFactoryBuilder().setNameFormat(namePrefix + "-%d").build();
    }
}
