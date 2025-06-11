package utils

import (
	"context"
	"fmt"
	"surface_query/xtgeo"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

type BlobFetcher struct {
	sasToken                string
	blobStoreBaseUri        string
	testWithContainerClient *container.Client
}

func NewBlobFetcher(sasToken string, blobStoreBaseUri string) *BlobFetcher {
	// !!!!!!!!!!!!!!
	// !!!!!!!!!!!!!!
	// !!!!!!!!!!!!!!
	containerUrl := blobStoreBaseUri + "?" + sasToken
	containerClient, err := container.NewClientWithNoCredential(containerUrl, nil)
	if err != nil {
		return nil
	}

	fetcher := BlobFetcher{sasToken, blobStoreBaseUri, containerClient}

	//fetcher := BlobFetcher{sasToken, blobStoreBaseUri}
	return &fetcher
}

// Download blob as bytes
func (bf BlobFetcher) FetchAsBytes(objectUuid string) ([]byte, error) {
	//byteArr, statusCode, err := fetchBlobBytes(bf.sasToken, bf.blobStoreBaseUri, objectUuid)
	byteArr, err := fetchBlobBytesUsingAzblob(bf.testWithContainerClient, objectUuid)

	if err != nil {
		return nil, err
	}
	// if statusCode != 200 {
	// 	return nil, fmt.Errorf("blob fetch returned http error: %v", statusCode)
	// }

	return byteArr, nil
}

// Download blob and deserialize to surface
func (bf BlobFetcher) FetchAsSurface(objectUuid string) (*xtgeo.Surface, error) {
	byteArr, err := bf.FetchAsBytes(objectUuid)
	if err != nil {
		return nil, err
	}

	surface, err := xtgeo.DeserializeBlobToSurface(byteArr)
	if err != nil {
		return nil, err
	}

	return surface, nil
}

func fetchBlobBytes(sasToken string, baseUrl string, objectUuid string) ([]byte, int, error) {

	blobUrl := baseUrl + "/" + objectUuid + "?" + sasToken

	bytesResp, statusCode, err := HttpGet(blobUrl)
	if err != nil {
		return nil, 500, err
	}

	return bytesResp, statusCode, nil

}

func fetchBlobBytesUsingAzblob(containerClient *container.Client, objectUuid string) ([]byte, error) {

	ctx := context.Background()

	blobClient := containerClient.NewBlockBlobClient(objectUuid)

	/*
		resp, err := blobClient.DownloadStream(ctx, nil)
		if err != nil {
			return nil, err
		}

		byteArr, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		return byteArr, nil
	*/

	props, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get blob properties: %w", err)
	}

	size := *props.ContentLength
	if size == 0 {
		return []byte{}, nil
	}

	byteArr := make([]byte, size)
	_, err = blobClient.DownloadBuffer(ctx, byteArr, &blob.DownloadBufferOptions{Concurrency: 1})
	if err != nil {
		return nil, err
	}

	return byteArr, nil
}
