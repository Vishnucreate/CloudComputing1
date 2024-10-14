package com.aws.ec2;

import java.io.IOException;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.springframework.boot.SpringApplication;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
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

public class AWSObjectRekognition {
    public static void main(String[] args) throws IOException, JMSException, InterruptedException {
        SpringApplication.run(AWSObjectRekognition.class, args);

        Regions clientRegion = Regions.US_EAST_1;
        String bucketName = "njit-cs-643";
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/323052225972/sqsforcarimage";

        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .build();

            // Create an SQS connection factory using default configurations
            SQSConnectionFactory connectionFactory = new SQSConnectionFactory(new ProviderConfiguration(),
                    AmazonSQSClientBuilder.defaultClient());

            // Establish the SQS connection
            SQSConnection connection = connectionFactory.createConnection();

            // Obtain a session from the connection
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a queue identity and specify the SQS queue URL
            Queue queue = session.createQueue(queueUrl);

            // Set up a message producer for the specified queue
            MessageProducer producer = session.createProducer(queue);
            System.out.println("Listing objects...");

            // Prepare and execute the S3 request to list objects in the specified bucket
            ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
            ListObjectsV2Result result;

            do {
                result = s3Client.listObjectsV2(req);
                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                    String photo = objectSummary.getKey();
                    AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();

                    // Create a Rekognition request to detect labels in the image
                    DetectLabelsRequest request = new DetectLabelsRequest()
                            .withImage(new Image().withS3Object(new S3Object().withName(photo).withBucket(bucketName)))
                            .withMaxLabels(10).withMinConfidence(75F);
                    try {
                        DetectLabelsResult detectLabelsResult = rekognitionClient.detectLabels(request);
                        List<Label> labels = detectLabelsResult.getLabels();

                        // Check for "Car" label with confidence > 90
                        for (Label label : labels) {
                            if (label.getName().equals("Car") && label.getConfidence() > 90) {
                                System.out.println("Detected labels for: " + photo + " => ");
                                System.out.println("Label: " + label.getName() + ", Confidence: " + label.getConfidence());

                                // Send a message with the image key to the specified SQS queue
                                TextMessage message = session.createTextMessage(objectSummary.getKey());
                                message.setStringProperty("JMSXGroupID", "Default");
                                producer.send(message);

                                System.out.println("Message sent with ID: " + message.getJMSMessageID());
                            }
                        }
                    } catch (AmazonRekognitionException e) {
                        e.printStackTrace();
                    }
                }
                req.setContinuationToken(result.getNextContinuationToken());
            } while (result.isTruncated());

        } catch (AmazonServiceException e) {
            e.printStackTrace();
        } catch (SdkClientException e) {
            e.printStackTrace();
        }
    }
}
