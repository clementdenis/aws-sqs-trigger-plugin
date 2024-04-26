package io.jenkins.plugins.sqs;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import lombok.SneakyThrows;

import java.util.concurrent.TimeUnit;

/**
 * This counter will sleep an increased amount of time every error, until it reaches max errors.
 *
 * Max sleep time is maxErrorCount(maxErrorCount + 1) / 2 * sleepTimeMultiplierSeconds.
 * For defaults: 5 * 6 / 2 * 10 = 150 seconds
 * @see https://en.wikipedia.org/wiki/1_%2B_2_%2B_3_%2B_4_%2B_%E2%8B%AF
 */
public class SleepingErrorCounter {

    private final Multiset<String> counter = ConcurrentHashMultiset.create();

    private final int maxErrorCount;

    private final int sleepTimeMultiplierSeconds;

    public SleepingErrorCounter() {
        this(5, 10);
    }

    public SleepingErrorCounter(int maxErrorCount, int sleepTimeMultiplierSeconds) {
        this.maxErrorCount = maxErrorCount;
        this.sleepTimeMultiplierSeconds = sleepTimeMultiplierSeconds;
    }

    void reset(String key) {
        counter.setCount(key, 0);
    }

    @SneakyThrows
    boolean tooManyErrors(String key) {
        boolean tooManyErrors = counter.add(key, 1) >= maxErrorCount;
        if (!tooManyErrors) {
            TimeUnit.SECONDS.sleep((long) sleepTimeMultiplierSeconds * counter.count(key));
        }
        return tooManyErrors;
    }

}
