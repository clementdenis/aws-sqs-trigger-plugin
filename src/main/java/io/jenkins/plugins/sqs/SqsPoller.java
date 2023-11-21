package io.jenkins.plugins.sqs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.model.Message;

import java.util.List;

public interface SqsPoller {
    void testConnection(String queueUrl, AWSCredentials credentialsId);

    List<Message> getMessagesAndDelete(String queueUrl, AWSCredentials awsCredentials, int waitTimeSeconds);
}
