package main

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/mtaimoor1/aws-utils/s3_utils"
)

func getS3Con() *s3.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	return s3.NewFromConfig(cfg)
}

func main() {
	s3_client := getS3Con()
	bucket := os.Getenv("bucket")
	prefix := os.Getenv("prefix")
	// destination_bucket := os.Getenv("destination_bucket")
	// destination_prefix := os.Getenv("destination_prefix")

	s3_utils.ListS3Objs(s3_client, bucket, prefix)
	// s3_utils.MoveS3Obj(s3_client, bucket, prefix, destination_bucket, destination_prefix)

}
