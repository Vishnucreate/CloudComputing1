package com.aws.ec2;

import java.io.IOException;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.springframework.boot.SpringApplication;

import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;

public class AWSObjectRekognition {

    public static void main(String[] args) throws IOException, JMSException {
        SpringApplication.run(AWSObjectRekognition.class, args);

        Regions clientRegion = Regions.US_EAST_1;
        String bucketName = "njit-cs-643";
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/323052225972/sqsforcarimage";

        try {
            AmazonSQS sqsClient = AmazonSQSClientBuilder.standard()
    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("https://sqs.us-east-1.amazonaws.com", "us-east-1"))
    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("ASIAUWN3JYW2O25BMFWE", "9rknlDDjY482pGPHBtE+XlqiMJYodE+yueBjlSUL"))) // Use your appropriate credential setup
    .build();


            // Set up the SQS connection factory with an explicit region
            SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
                new ProviderConfiguration(),
                AmazonSQSClientBuilder.standard()
                    .withRegion(clientRegion)  // Ensure correct region
            );

            // Establish the SQS connection
            SQSConnection connection = connectionFactory.createConnection();

            // Create a session with AUTO_ACKNOWLEDGE mode
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(queueUrl);  // Directly using the queue URL

            // Create a producer for the specified queue
            MessageProducer producer = session.createProducer(queue);

            // List objects in the specified S3 bucket
            ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
            ListObjectsV2Result result;

            do {
                result = s3Client.listObjectsV2(req);
                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                    String photo = objectSummary.getKey();
                    AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.standard()
                        .withRegion(clientRegion)
                        .build();

                    // Rekognition request to detect labels in the image
                    DetectLabelsRequest detectLabelsRequest = new DetectLabelsRequest()
                            .withImage(new Image().withS3Object(new S3Object().withName(photo).withBucket(bucketName)))
                            .withMaxLabels(10)
                            .withMinConfidence(75F);

                    try {
                        DetectLabelsResult detectLabelsResult = rekognitionClient.detectLabels(detectLabelsRequest);
                        List<Label> labels = detectLabelsResult.getLabels();

                        for (Label label : labels) {
                            if (label.getName().equals("Car") && label.getConfidence() > 90) {
                                System.out.println("Detected 'Car' with confidence: " + label.getConfidence());
                                TextMessage message = session.createTextMessage(photo);
                                message.setStringProperty("JMSXGroupID", "Default");  // Group ID for FIFO queue
                                producer.send(message);

                                System.out.println("Message sent with ID: " + message.getJMSMessageID());
                            }
                        }
                    } catch (AmazonRekognitionException e) {
                        System.err.println("Rekognition error for image: " + photo);
                        e.printStackTrace();
                    }
                }
                req.setContinuationToken(result.getNextContinuationToken());
            } while (result.isTruncated());

        } catch (AmazonServiceException e) {
            System.err.println("AWS service error:");
            e.printStackTrace();
        } catch (SdkClientException e) {
            System.err.println("AWS SDK client error:");
            e.printStackTrace();
        }
    }
}
