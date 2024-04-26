package io.jenkins.plugins.sqs;

import com.google.common.base.Stopwatch;
import junit.framework.TestCase;

public class SleepingErrorCounterTest extends TestCase {

    public void testTooManyErrors() {
        SleepingErrorCounter counter = new SleepingErrorCounter(2, 1);
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        assertFalse(counter.tooManyErrors("key"));
        assertFalse(counter.tooManyErrors("key"));
        assertTrue(counter.tooManyErrors("key"));
        stopwatch.stop();
        int expectedSleepTimeMillis = 2 * 3 / 2 * 1000;
        long elapsedMillis = stopwatch.elapsedMillis();
        assertTrue(elapsedMillis >= expectedSleepTimeMillis && elapsedMillis < expectedSleepTimeMillis + 1000);
    }

}