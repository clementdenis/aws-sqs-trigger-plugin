package io.jenkins.plugins.sqs;

import hudson.util.CopyOnWriteMap;
import lombok.extern.java.Log;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@Log
public class AllTriggers {

    public static final AllTriggers INSTANCE = new AllTriggers();

    private AllTriggers() {
    }

    private final Map<String, SqsTrigger> triggerList = new CopyOnWriteMap.Hash<>();

    public void add(SqsTrigger trigger) {
        log.fine(() -> "Add SQS trigger, url: " + trigger.getSqsTriggerQueueUrl() + " job: " + trigger.getJobFullName() + ".");
        triggerList.put(trigger.getSqsTriggerQueueUrl(), trigger);
    }

    public void remove(SqsTrigger trigger) {
        log.fine(() -> "Remove SQS trigger, url: " + trigger.getSqsTriggerQueueUrl() + " job: " + trigger.getJobFullName() + ".");
        triggerList.remove(trigger.getSqsTriggerQueueUrl());
    }

    public Collection<SqsTrigger> getAll() {
        return triggerList.values();
    }

}
