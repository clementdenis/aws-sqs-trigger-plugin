package io.jenkins.plugins.sqs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import jenkins.model.Jenkins;
import lombok.SneakyThrows;
import lombok.extern.java.Log;

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Level;
import java.util.stream.Collectors;

@Log
public class SqsPollerImpl implements SqsPoller {

    private static final SleepingErrorCounter ERROR_COUNTER = new SleepingErrorCounter();

    @Override
    public void testConnection(String queueUrl, AWSCredentials credentialsId) {
        AmazonSQS sqs = createSQSClient(queueUrl, credentialsId);
        receiveMessages(queueUrl, sqs, 0);
    }

    /**
     *
     * @return the messages, or null if queue should be deregistered
     */
    @Override
    public List<Message> getMessagesAndDelete(String queueUrl, AWSCredentials awsCredentials, int waitTimeSeconds) {
        try {
            AmazonSQS sqs = createSQSClient(queueUrl, awsCredentials);

            List<Message> messages = receiveMessages(queueUrl, sqs, waitTimeSeconds);

            if (!messages.isEmpty()) {
                deleteMessages(queueUrl, sqs, messages);
            }

            ERROR_COUNTER.reset(queueUrl);
            return messages;
        } catch (QueueDoesNotExistException e) {
            log.log(Level.WARNING, "Queue " + queueUrl + " does not exist anymore.", e);
            return null;
        } catch (Exception e) {
            String message = "Unexpected error retrieving messages from " + queueUrl + ".";
            if (ERROR_COUNTER.tooManyErrors(queueUrl)) {
                log.log(Level.WARNING, message, e);
                return null;
            }
            log.log(Level.FINE, message, e);
            return Collections.emptyList();
        }
    }

    private void deleteMessages(String queueUrl, AmazonSQS sqs, List<Message> messages) {
        log.fine(() -> "Start to delete SQS " + messages.size() + " message(s) from " + queueUrl + ".");
        List<DeleteMessageBatchRequestEntry> entries = messages.stream()
                .map(message -> {
                    UUID uuid = UUID.randomUUID();
                    return new DeleteMessageBatchRequestEntry(uuid.toString(), message.getReceiptHandle());
                })
                .collect(Collectors.toList());
        sqs.deleteMessageBatch(queueUrl, entries);
        log.fine(() -> "Message deleted from " + queueUrl + ".");
    }

    private List<Message> receiveMessages(String queueUrl, AmazonSQS sqs, int waitTimeSeconds) {

        log.fine(() -> "Start to receive SQS message from " + queueUrl + " with waitTimeSeconds " + waitTimeSeconds + ".");
        ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withWaitTimeSeconds(waitTimeSeconds)
                .withMaxNumberOfMessages(10);
        ReceiveMessageResult messageResult = sqs.receiveMessage(receiveRequest);
        List<Message> messages = messageResult.getMessages();
        log.fine(() -> messages.size() + " message(s) received from " + queueUrl + ".");
        return messages;
    }

    @SneakyThrows
    private AmazonSQS createSQSClient(String queueUrl, AWSCredentials awsCredentials) {
        AWSCredentialsProvider credentialsProvider;

        log.finest(() -> "Guess region from " + queueUrl + ".");
        String sqsFQDN = new URL(queueUrl).getHost();
        String region = sqsFQDN.split("\\.")[1];
        log.finest(() -> "Use " + region + ".");

        if (awsCredentials != null) {
            log.finest(() -> "Use the AWS credentials " + awsCredentials.getAWSAccessKeyId() + ".");
            credentialsProvider = new AWSCredentialsProvider() {
                @Override
                public AWSCredentials getCredentials() {
                    return awsCredentials;
                }

                @Override
                public void refresh() {
                }
            };
        } else {
            log.finest(() -> "Use default credentials chain.");
            credentialsProvider = new DefaultAWSCredentialsProviderChain();
        }

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        Jenkins jenkins = Jenkins.getInstanceOrNull();

        Optional.ofNullable(jenkins)
                .map(Jenkins::getProxy)
                .ifPresent(proxyConfiguration -> {
                    log.finest(() -> "Use Jenkins Proxy.");
                    clientConfiguration.setProxyHost(proxyConfiguration.getName());
                    clientConfiguration.setProxyPort(proxyConfiguration.getPort());
                    clientConfiguration.setNonProxyHosts(proxyConfiguration.getNoProxyHost());
                });


        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withClientConfiguration(clientConfiguration)
                .withCredentials(credentialsProvider)
                .withRegion(region)
                .build();
        return sqs;
    }


}
