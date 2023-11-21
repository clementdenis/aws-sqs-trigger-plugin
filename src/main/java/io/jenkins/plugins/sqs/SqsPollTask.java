package io.jenkins.plugins.sqs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.model.Message;
import com.google.common.base.Stopwatch;
import hudson.Extension;
import hudson.model.AsyncPeriodicWork;
import hudson.model.TaskListener;
import lombok.extern.java.Log;
import org.apache.commons.lang.time.StopWatch;
import org.jenkinsci.Symbol;
import org.kohsuke.accmod.Restricted;
import org.kohsuke.accmod.restrictions.NoExternalUse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;


@Extension
@Restricted(NoExternalUse.class)
@Symbol("sqsPollTask")
@Log
public class SqsPollTask extends AsyncPeriodicWork {

    public static final int RECURRENCE_PERIOD_SECONDS = 600;

    private transient SqsPoller sqsPoller = new SqsPollerImpl();

    public SqsPollTask() {
        super("sqsPollTask");
    }

    @Override
    protected void execute(TaskListener listener) throws IOException, InterruptedException {
        long startedAt = System.currentTimeMillis();
        IntSupplier remainingSeconds = () -> RECURRENCE_PERIOD_SECONDS - (int) (System.currentTimeMillis() - startedAt) / 1000;

        List<SqsTrigger> triggers = AllTriggers.INSTANCE.getAll();
        log.fine(() -> "Found " + triggers.size() + " SQS triggers.");

        ExecutorService executorService = Executors.newFixedThreadPool(triggers.size());
        try {
            triggers.forEach(trigger -> executorService.submit(() -> {
                while (remainingSeconds.getAsInt() > 5) {
                    String credentialsId = trigger.getSqsTriggerCredentialsId();
                    AWSCredentials awsCredentials = AwsCredentialsHelper.getAWSCredentials((credentialsId));
                    int waitTimeSeconds = Math.min(20, remainingSeconds.getAsInt());
                    List<Message> messages = sqsPoller.getMessagesAndDelete(trigger.getSqsTriggerQueueUrl(), awsCredentials, waitTimeSeconds);
                    trigger.buildJob(messages);
                }
            }));
        } finally {
            executorService.shutdown();
        }

    }

    public void setSqsPoller(SqsPoller sqsPoller) {
        this.sqsPoller = sqsPoller;
    }

    @Override
    public long getInitialDelay() {
        return 0;
    }

    @Override
    public long getRecurrencePeriod() {
        return TimeUnit.SECONDS.toMillis(RECURRENCE_PERIOD_SECONDS);
    }


}
