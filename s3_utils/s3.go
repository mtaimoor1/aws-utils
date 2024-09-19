package s3_utils

import (
	"context"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Wait group for concurrent requests
var wg sync.WaitGroup

// Rename files in s3
func RenameS3Obj(s3_client *s3.Client, bucket string, prefix string, new_name string) {

	s3_client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		CopySource: aws.String(bucket + "/" + prefix),
		Bucket:     aws.String(bucket),
		Key:        aws.String(new_name),
	})

	s3_client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(prefix),
	})

}

// Displays the key name, size and modification date from the list of object
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

// Moves all the objects inside a prefix to a certain destination prefix.
// WARNING: This function will DELETE the data from the orignal location
func MoveS3Obj(s3_client *s3.Client, s_bucket string, s_prefix string, d_bucket string, d_prefix string) {
	res, err := getListOfS3Obj(s3_client, s_bucket, s_prefix)
	if err != nil {
		log.Fatal(err)
	}
	for _, object := range res {
		wg.Add(1)
		go moveObj(s3_client, d_bucket, d_prefix, s_bucket, object)
	}
	wg.Wait()
}

// Function to make the API calls to move the object
// Wrote this function separately for encapsulation and to run in goroutine
func moveObj(s3_client *s3.Client, d_bucket string, d_prefix string, s_bucket string, obj types.Object) {
	defer wg.Done()
	s3_client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		CopySource: aws.String(s_bucket + "/" + *obj.Key),
		Bucket:     aws.String(d_bucket),
		Key:        aws.String(d_prefix + *obj.Key),
	})
	s3_client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(s_bucket),
		Key:    aws.String(*obj.Key),
	})
}

// Function to get the list of s3 objects at a certain perfix
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
