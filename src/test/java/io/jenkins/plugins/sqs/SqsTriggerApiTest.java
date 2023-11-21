package io.jenkins.plugins.sqs;

import com.amazonaws.auth.AWSCredentials;
import com.cloudbees.jenkins.plugins.awscredentials.AWSCredentialsImpl;
import com.cloudbees.jenkins.plugins.awscredentials.AmazonWebServicesCredentials;
import com.cloudbees.plugins.credentials.CredentialsScope;
import com.cloudbees.plugins.credentials.SystemCredentialsProvider;
import hudson.model.FreeStyleProject;
import jenkins.model.Jenkins;
import org.junit.Rule;
import org.junit.Test;
import org.jvnet.hudson.test.JenkinsSessionRule;
import org.mockito.ArgumentCaptor;

import java.util.Collections;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;


public class SqsTriggerApiTest {

    @Rule
    public JenkinsSessionRule sessions = new JenkinsSessionRule();


    @Test
    public void pollTest() throws Throwable {
        String awsCredentialId = "aws-id";
        String accessKey = "aws-access-key";
        String secretKey = "aws-secret-key";
        sessions.then(r -> {
            Jenkins jenkins = r.getInstance();

            AmazonWebServicesCredentials awsCredentials = new AWSCredentialsImpl(CredentialsScope.GLOBAL, awsCredentialId, accessKey, secretKey, "aws");
            SystemCredentialsProvider systemCredentialsProvider = r.getInstance().getExtensionList(SystemCredentialsProvider.class).get(0);
            systemCredentialsProvider.getCredentials().add(awsCredentials);
            systemCredentialsProvider.save();

            FreeStyleProject testProject = jenkins.createProject(FreeStyleProject.class, "test");
            SqsTrigger sqsTrigger = new SqsTrigger();
            sqsTrigger.setSqsTriggerCredentialsId(awsCredentials.getId());
            sqsTrigger.setSqsTriggerQueueUrl("test-queue-url");
            testProject.addTrigger(sqsTrigger);
            testProject.save();


        });

        sessions.then(r -> {
            Jenkins jenkins = r.getInstance();
            SqsPollTask sqsPollTask = jenkins.getExtensionList(SqsPollTask.class).get(0);
            SqsPoller mockSqsPoller = mock(SqsPoller.class);
            when(mockSqsPoller.getMessagesAndDelete(any(String.class), any(), 20))
                    .thenReturn(Collections.emptyList());
            sqsPollTask.setSqsPoller(mockSqsPoller);
            await().atMost(60, SECONDS).untilAsserted(() -> {
                ArgumentCaptor<AWSCredentials> captorAWSCredentials = ArgumentCaptor.forClass(AWSCredentials.class);
                verify(mockSqsPoller).getMessagesAndDelete(eq("test-queue-url"), captorAWSCredentials.capture(), 20);
                AWSCredentials capturedAWSCredentials = captorAWSCredentials.getValue();
                assertThat(capturedAWSCredentials.getAWSAccessKeyId()).isEqualTo(accessKey);
                assertThat(capturedAWSCredentials.getAWSSecretKey()).isEqualTo(secretKey);

            });
        });
    }


}
