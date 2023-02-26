package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func uploadToS3(csvFiles map[string]string) {
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	bucketName := "sample-customer1-" + time.Now().Format("2006-01-02")

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("ap-southeast-2"),
		Credentials: credentials.NewStaticCredentials(accessKey, secretKey, ""),
	})

	if err != nil {
		panic(err)
	}

	svc := s3.New(sess)

	_, err = svc.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		fmt.Println("Creating bucket", bucketName)
		_, createErr := svc.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if createErr != nil {
			panic(createErr)
		}
	}

	uploader := s3manager.NewUploader(sess)

	for _, fileName := range csvFiles {
		fmt.Println("Uploading file", fileName, "to bucket", bucketName)
		f, err := os.Open(fileName)
		if err != nil {
			fmt.Println(fmt.Errorf("failed to open file %q, %v", fileName, err))
		}

		_, err = uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(fileName[5:]),
			Body:   f,
		})

		if err != nil {
			panic(err)
		}
	}
}
