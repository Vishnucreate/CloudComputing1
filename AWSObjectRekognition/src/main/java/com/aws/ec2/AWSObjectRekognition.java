package com.aws.instanceA;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.DetectLabelsRequest;
import software.amazon.awssdk.services.rekognition.model.Image;
import software.amazon.awssdk.services.rekognition.model.DetectLabelsResponse;
import software.amazon.awssdk.services.rekognition.model.Label;
import software.amazon.awssdk.services.rekognition.model.S3Object;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.util.List;

public class AWSObjectRekognition 
{
  
   public static void main( String[] args )
    {
     Region region = Region.US_EAST_1; 
        
   
        RekognitionClient rekognitionClient = RekognitionClient.builder()
                .region(region)
                .build();
        
        SqsClient sqsClient = SqsClient.builder()
                .region(region)
                .build();
        
        S3Client s3Client = S3Client.builder()
                .region(region)
                .build();
        
    
        String sqsQueueUrl = "https://sqs.us-east-1.amazonaws.com/323052225972/sqsforcarimage";
        
        String bucketName = "njit-cs-643"; 
        

        List<String> imageNames = fetchImageNamesFromS3(bucketName, s3Client);
        for (String imageName : imageNames) {
            detectCars(bucketName, imageName, rekognitionClient, sqsClient, sqsQueueUrl);
        }
        
  
        rekognitionClient.close();
        sqsClient.close();
        s3Client.close();
    }


    public static List<String> fetchImageNamesFromS3(String bucketName, S3Client s3Client) {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();

        ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
        return listResponse.contents().stream()
                .map(s3Object -> s3Object.key())
                .toList();
    }


    public static void detectCars(String bucketName, String imageName, RekognitionClient rekognitionClient, SqsClient sqsClient, String sqsQueueUrl) {

        S3Object s3Object = S3Object.builder()
                .bucket(bucketName)
                .name(imageName)
                .build();

        
        Image image = Image.builder()
                .s3Object(s3Object)
                .build();


        DetectLabelsRequest request = DetectLabelsRequest.builder()
                .image(image)
                .minConfidence(90F) 
                .build();

        
        DetectLabelsResponse result = rekognitionClient.detectLabels(request);
        List<Label> labels = result.labels();

        for (Label label : labels) {
            if (label.name().equalsIgnoreCase("Car") && label.confidence() >= 90) {
                System.out.println("Car detected in image: " + imageName + " with confidence: " + label.confidence());
                pushToSQS(imageName, sqsClient, sqsQueueUrl);
                break; 
            }
        }
    }

    public static void pushToSQS(String imageName, SqsClient sqsClient, String sqsQueueUrl) {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(sqsQueueUrl)
                .messageBody(imageName)
                .build();

        SendMessageResponse sendMessageResponse = sqsClient.sendMessage(sendMessageRequest);
        System.out.println("Message sent to SQS: " + imageName + ", MessageId: " + sendMessageResponse.messageId());
    }
}
