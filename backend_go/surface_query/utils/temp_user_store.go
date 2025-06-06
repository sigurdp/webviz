package utils

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/redis/go-redis/v9"
)

// Constants scoped to this file
const (
	tus_redisKeyPrefix    = "temp_user_store_index"
	tus_blobContainerName = "test-user-scoped-temp-storage"
)

type TempUserStoreFactory struct {
	redisClient     *redis.Client
	containerClient *container.Client
	ttlDuration     time.Duration
}

type TempUserStore struct {
	redisClient     *redis.Client
	containerClient *container.Client
	ttlDuration     time.Duration
	userId          string
}

// Creates factory for later creation of TempUserStore instances
// This is useful to avoid creating a new Redis client and Azure Blob Storage client for each user
// Note that function will panic if it fails to create the Redis or Azure Blob Storage clients
func NewTempUserStoreFactory(redisUrl string, storageAccountConnStr string, ttlInSeconds int) *TempUserStoreFactory {
	slog.Info("Creating TempUserStoreFactory", "redisUrl", redisUrl, "blobContainerName", tus_blobContainerName, "ttl(s)", ttlInSeconds)

	redisOpts, err := redis.ParseURL(redisUrl)
	if err != nil {
		panic(fmt.Sprintf("failed to parse Redis URL: %v", err))
	}

	redisClient := redis.NewClient(redisOpts)

	containerClient, err := container.NewClientFromConnectionString(storageAccountConnStr, tus_blobContainerName, nil)
	if err != nil {
		panic(fmt.Sprintf("failed to create container client: %v", err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		panic(fmt.Sprintf("failed to connect to Redis: %v", err))
	}

	if _, err := containerClient.GetProperties(ctx, nil); err != nil {
		panic(fmt.Sprintf("failed to access blob container: %v", err))
	}

	return &TempUserStoreFactory{
		redisClient:     redisClient,
		containerClient: containerClient,
		ttlDuration:     time.Duration(ttlInSeconds) * time.Second,
	}
}

// Returns a user scoped temp store for a given user.
func (f *TempUserStoreFactory) ForUser(userId string) *TempUserStore {
	if userId == "" {
		panic("userId must not be empty")
	}

	return &TempUserStore{
		userId:          userId,
		redisClient:     f.redisClient,
		containerClient: f.containerClient,
		ttlDuration:     f.ttlDuration,
	}
}

func (s *TempUserStore) PutBytes(ctx context.Context, key string, payloadBytes []byte, blobPrefix string, blobExtension string) error {

	payloadHash := computePayloadHash(payloadBytes)

	blobName := "user__" + s.userId + "/"
	if blobPrefix != "" {
		blobName += blobPrefix + "---"
	}
	blobName += "sha__" + payloadHash
	if blobExtension != "" {
		blobName += "." + blobExtension
	}

	err := uploadOrUpdateMetadataForBlob(ctx, s.containerClient, blobName, payloadBytes)
	if err != nil {
		return fmt.Errorf("failed to upload or refresh blob: %w", err)
	}

	// For now, mimick the prefix in aiocache
	// redo this when we switch away from aiocache, but we still probably want a prefix for the Redis keys
	redisKey := s.makeFullRedisKey(key)
	slog.Info("REDIS key", "key", redisKey, "blobName", blobName)

	err = s.redisClient.Set(ctx, redisKey, blobName, s.ttlDuration).Err()
	if err != nil {
		return fmt.Errorf("failed to set Redis key: %w", err)
	}

	return nil
}

func (s *TempUserStore) makeFullRedisKey(key string) string {
	return tus_redisKeyPrefix + ":user:" + s.userId + ":" + key
}

// Computes the SHA256 hash of the payload bytes and returns the hash as a hex-encoded string.
func computePayloadHash(payload []byte) string {
	checksum := sha256.Sum256(payload)
	return hex.EncodeToString(checksum[:])
}

func uploadOrUpdateMetadataForBlob(ctx context.Context, containerClient *container.Client, blobName string, payloadBytes []byte) error {
	blobClient := containerClient.NewBlockBlobClient(blobName)

	// Check if blob already exists
	_, err := blobClient.GetProperties(ctx, nil)
	if err == nil {
		// Blob exists — do an update of the blob's metadata
		// The assumption here is that we have a lifecycle policy on the blob container that deletes blobs based on the last modified time,
		// so we want to keep this blob alive by updating its metadata (which in turn updates the last modified time)
		timeNow := time.Now().UTC().Format(time.RFC3339)
		metadata := map[string]*string{"refreshedAt": &timeNow}

		slog.Debug("uploadOrUpdateMetadataForBlob() - blob already exists, updating metadata")
		_, err := blobClient.SetMetadata(ctx, metadata, nil)
		if err != nil {
			return fmt.Errorf("updating metadata failed: %w", err)
		}
		return nil
	}

	// Blob does not exist, so upload it
	slog.Debug("uploadOrUpdateMetadataForBlob() - uploading blob")

	neverStr := "never"
	metadata := map[string]*string{"refreshedAt": &neverStr}
	_, err = blobClient.UploadBuffer(ctx, payloadBytes, &azblob.UploadBufferOptions{Metadata: metadata})
	if err != nil {
		return fmt.Errorf("blob upload failed: %w", err)
	}

	return nil
}
