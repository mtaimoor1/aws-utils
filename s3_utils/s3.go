package s3_utils

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func getListOfS3Obj(s3_client *s3.Client, bucket string, prefix string) ([]types.Object, error) {

	result := []types.Object{}

	list_payload := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	for {
		output, err := s3_client.ListObjectsV2(context.TODO(), list_payload)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}

		result = append(result, output.Contents...)

		if *output.IsTruncated == true {
			list_payload.ContinuationToken = output.NextContinuationToken
		} else {
			break
		}
	}
	return result, nil
}

func ListS3Objs(s3_client *s3.Client, bucket string, prefix string) {

	results, err := getListOfS3Obj(s3_client, bucket, prefix)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Objects from %s are as follows:\n", bucket)
	for _, obj := range results {
		log.Printf("key: %s size %d date %s", *obj.Key, obj.Size, obj.LastModified)
	}
}

func MoveS3Obj(s3_client *s3.Client, s_bucket string, s_prefix string, d_bucket string, d_prefix string) []types.Object {
	res, err := getListOfS3Obj(s3_client, s_bucket, s_prefix)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	for _, obj := range res {
		s3_client.CopyObject(context.TODO(), &s3.CopyObjectInput{
			Bucket:     aws.String(d_bucket),
			CopySource: aws.String(s_bucket + "/" + *obj.Key),
			Key:        aws.String(d_prefix + *obj.Key),
		})
		s3_client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(s_bucket),
			Key:    aws.String(*obj.Key),
		})
	}
	return res
}
