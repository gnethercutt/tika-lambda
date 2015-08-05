package org.nethercutt.aws.lambda;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public abstract class TikaLambdaHandler implements RequestHandler<S3Event, String> {

    public String handleRequest(S3Event s3event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("received : " + s3event.toJson());
        Tika tika = new Tika();

        try {
            S3EventNotificationRecord record = s3event.getRecords().get(0);

            String bucket = record.getS3().getBucket().getName();

            // Object key may have spaces or unicode non-ASCII characters.
            String key = URLDecoder.decode(record.getS3().getObject().getKey().replace('+', ' '), "UTF-8");

            AmazonS3 s3Client = new AmazonS3Client();
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, key));
            
            try (InputStream objectData = s3Object.getObjectContent()) {
                String extractedText = tika.parseToString(objectData);
                processExtractedText(s3event, extractedText);
            }
        } catch (IOException | TikaException e) {
            logger.log("Exception: " + e.getLocalizedMessage());
            throw new RuntimeException(e);
        }
        return "Ok";
    }
    
    public abstract void processExtractedText(S3Event s3event, String extractedText);
}
