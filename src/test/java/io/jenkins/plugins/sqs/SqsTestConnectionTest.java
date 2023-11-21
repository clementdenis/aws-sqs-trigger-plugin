package io.jenkins.plugins.sqs;

import com.amazonaws.auth.AWSCredentials;
import com.cloudbees.jenkins.plugins.awscredentials.AWSCredentialsImpl;
import com.cloudbees.jenkins.plugins.awscredentials.AmazonWebServicesCredentials;
import com.cloudbees.plugins.credentials.CredentialsScope;
import com.cloudbees.plugins.credentials.SystemCredentialsProvider;
import com.gargoylesoftware.htmlunit.html.*;
import hudson.model.FreeStyleProject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.jvnet.hudson.test.JenkinsRule;
import org.jvnet.hudson.test.JenkinsSessionRule;
import org.mockito.ArgumentCaptor;

import java.util.Collections;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;


public class SqsTestConnectionTest {

    public static final String ACCESS_KEY = "awskey";
    public static final String SECRET_KEY = "awssecret";
    @Rule
    public JenkinsSessionRule sessions = new JenkinsSessionRule();

    private AmazonWebServicesCredentials awsCredentials;
    private FreeStyleProject project;

    @Before
    public void setup() throws Throwable {
        sessions.then(r -> {
            awsCredentials = new AWSCredentialsImpl(CredentialsScope.GLOBAL, "aws", ACCESS_KEY, SECRET_KEY, "aws");
            SystemCredentialsProvider systemCredentialsProvider = r.getInstance().getExtensionList(SystemCredentialsProvider.class).get(0);
            systemCredentialsProvider.getCredentials().add(awsCredentials);
            systemCredentialsProvider.save();
            project = r.getInstance().createProject(FreeStyleProject.class, "test");
        });
    }

    @Test
    public void testConnectionTest() throws Throwable {

        sessions.then(r -> {
            SqsPoller mockSqsPoller = mock(SqsPoller.class);
            when(mockSqsPoller.getMessagesAndDelete(any(String.class), any(), 20))
                    .thenReturn(Collections.emptyList());
            SqsTrigger.SqsTriggerDescriptor sqsTriggerDescriptor=r.getInstance().getExtensionList(SqsTrigger.SqsTriggerDescriptor.class).get(0);
            sqsTriggerDescriptor.setSqsPoller(mockSqsPoller);
            JenkinsRule.WebClient webClient = r.createWebClient();
            HtmlForm config = webClient.goTo(project.getUrl() + "/configure").getFormByName("config");
            HtmlCheckBoxInput sqsTriggerCheckBox = config.getInputByName("io-jenkins-plugins-sqs-SqsTrigger");

            sqsTriggerCheckBox.setChecked(true);
            HtmlTextInput queueNameInput = config.getInputByName("_.sqsTriggerQueueUrl");
            queueNameInput.setText("some-aws-queue-url");
            HtmlSelect credentials = config.getSelectByName("_.sqsTriggerCredentialsId");
            credentials.setSelectedAttribute("aws", true);

            HtmlButton button=  config.getOneHtmlElementByAttribute("button",
                    "data-validate-button-descriptor-url",
                    "/jenkins/job/test/descriptorByName/io.jenkins.plugins.sqs.SqsTrigger");
            button.click(false,false,false,true,true,false);
            await().atMost(10, SECONDS).untilAsserted(() -> {
                ArgumentCaptor<AWSCredentials> captorAWSCredentials = ArgumentCaptor.forClass(AWSCredentials.class);
                verify(mockSqsPoller).testConnection(eq("some-aws-queue-url"), captorAWSCredentials.capture());
                AWSCredentials capturedAWSCredentials = captorAWSCredentials.getValue();
                assertThat(capturedAWSCredentials.getAWSAccessKeyId()).isEqualTo(ACCESS_KEY);
                assertThat(capturedAWSCredentials.getAWSSecretKey()).isEqualTo(SECRET_KEY);

            });


        });


    }


}
