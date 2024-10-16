import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazon.sqs.javamessaging.*;
import javax.jms.*;
import java.io.IOException;
import java.util.List;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AWSObjectRekognition {

    public static void main(String[] args) throws IOException, JMSException {
        SpringApplication.run(AWSObjectRekognition.class, args);
        String bucketName = "njit-cs-643";
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/323052225972/sqsforcarimage";

        try {
            System.out.println("Initializing S3 client...");
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new DefaultAWSCredentialsProviderChain())
                    .build();
            System.out.println("S3 client initialized.");

            System.out.println("Setting up SQS connection factory...");
            SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
                    new ProviderConfiguration(),
                    AmazonSQSClientBuilder.standard()
                            .withCredentials(new DefaultAWSCredentialsProviderChain())
                            .withRegion("us-east-1")
            );
            System.out.println("SQS connection factory set up.");

            System.out.println("Establishing SQS connection...");
            SQSConnection connection = connectionFactory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            System.out.println("SQS connection established.");

            System.out.println("Creating queue from URL: " + queueUrl);
            Queue queue = session.createQueue(queueUrl);
            MessageProducer producer = session.createProducer(queue);
            System.out.println("Queue and producer created.");

            ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
            ListObjectsV2Result result;

            System.out.println("Listing objects in S3 bucket: " + bucketName);
            do {
                result = s3Client.listObjectsV2(req);
                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                    String photo = objectSummary.getKey();
                    System.out.println("Processing photo: " + photo);

                    System.out.println("Initializing Rekognition client...");
                    AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.standard()
                            .withCredentials(new DefaultAWSCredentialsProviderChain())
                            .withRegion("us-east-1")
                            .build();
                    System.out.println("Rekognition client initialized.");

                    DetectLabelsRequest detectLabelsRequest = new DetectLabelsRequest()
                            .withImage(new Image().withS3Object(new S3Object().withName(photo).withBucket(bucketName)))
                            .withMaxLabels(10)
                            .withMinConfidence(75F);
                    System.out.println("DetectLabelsRequest created for photo: " + photo);

                    // Call Rekognition to detect labels
                    DetectLabelsResult detectLabelsResult = rekognitionClient.detectLabels(detectLabelsRequest);
                    List<Label> labels = detectLabelsResult.getLabels();
                    System.out.println("Labels detected for photo: " + photo);

                    for (Label label : labels) {
                        System.out.println("Label found: " + label.getName() + " with confidence: " + label.getConfidence());
                        if ("Car".equals(label.getName()) && label.getConfidence() > 90) {
                            System.out.println("Car detected with confidence > 90. Sending to SQS queue.");

                            TextMessage message = session.createTextMessage(photo);
                            message.setStringProperty("JMSXGroupID", "Default");
                            producer.send(message);

                            System.out.println("Message sent with ID: " + message.getJMSMessageID());
                        }
                    }
                }
                req.setContinuationToken(result.getNextContinuationToken());
                System.out.println("Continuation token set for next batch.");
            } while (result.isTruncated());

            System.out.println("Closing connection...");
            connection.close();
            System.out.println("Connection closed.");

        } catch (AmazonServiceException e) {
            System.err.println("AWS service error:");
            e.printStackTrace();
        } catch (SdkClientException e) {
            System.err.println("AWS SDK client error:");
            e.printStackTrace();
        }
    }
}
