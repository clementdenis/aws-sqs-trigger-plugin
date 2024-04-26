package io.jenkins.plugins.sqs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.model.Message;
import hudson.Extension;
import hudson.model.AsyncPeriodicWork;
import hudson.model.TaskListener;
import lombok.extern.java.Log;
import org.jenkinsci.Symbol;
import org.kohsuke.accmod.Restricted;
import org.kohsuke.accmod.restrictions.NoExternalUse;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.logging.Level;


@Extension
@Restricted(NoExternalUse.class)
@Symbol("sqsPollTask")
@Log
public class SqsPollTask extends AsyncPeriodicWork {

    private static final int RECURRENCE_PERIOD_SECONDS = 600;
    private static final SleepingErrorCounter ERROR_COUNTER = new SleepingErrorCounter();

    private transient SqsPoller sqsPoller = new SqsPollerImpl();

    public SqsPollTask() {
        super("sqsPollTask");
    }

    @Override
    protected void execute(TaskListener listener) throws IOException, InterruptedException {
        long startedAt = System.currentTimeMillis();
        IntSupplier remainingSeconds = () -> RECURRENCE_PERIOD_SECONDS - (int) (System.currentTimeMillis() - startedAt) / 1000;

        Collection<SqsTrigger> triggers = AllTriggers.INSTANCE.getAll();
        log.fine(() -> "Found " + triggers.size() + " SQS triggers.");

        ExecutorService executorService = Executors.newFixedThreadPool(triggers.size());
        try {
            triggers.forEach(trigger -> executorService.submit(() -> {
                try {
                    while (remainingSeconds.getAsInt() > 5) {
                        String credentialsId = trigger.getSqsTriggerCredentialsId();
                        AWSCredentials awsCredentials = AwsCredentialsHelper.getAWSCredentials((credentialsId));
                        int waitTimeSeconds = Math.min(20, remainingSeconds.getAsInt());
                        List<Message> messages = sqsPoller.getMessagesAndDelete(trigger.getSqsTriggerQueueUrl(),
                                awsCredentials, waitTimeSeconds);
                        if (messages == null) {
                            log.info("Could not get messages for queue " + trigger.getSqsTriggerQueueUrl() + ", deregistering");
                            AllTriggers.INSTANCE.remove(trigger);
                        } else {
                            trigger.buildJob(messages);
                            ERROR_COUNTER.reset(trigger.getSqsTriggerQueueUrl());
                        }
                    }
                } catch (Exception e) {
                    if (ERROR_COUNTER.tooManyErrors(trigger.getSqsTriggerQueueUrl())) {
                        log.log(Level.SEVERE, "Failed to launch job from message on queue " + trigger.getSqsTriggerQueueUrl(), e);
                        AllTriggers.INSTANCE.remove(trigger);
                    } else {
                        log.log(Level.WARNING, "Failed to launch job from message on queue " + trigger.getSqsTriggerQueueUrl(), e);
                    }
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
