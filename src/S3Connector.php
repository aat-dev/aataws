<?php

namespace AAT\AWS;

use Aws\S3\S3Client;

/**
 * Class S3Connector
 * @package AAT\AWS
 */
class S3Connector {

  private $config;
  private $client;

  /**
   * constructor for S3Client
   * @param $credentials
   */
  function __construct($credentials) {
    $this->client = S3Client::factory($credentials);
  }

  /**
   * List buckets
   * @return \Aws\Result
   */
  function listBuckets() {
    return $this->client->ListBuckets();
  }

  /**
   * Copy object
   * @param $src
   * @param $dst
   */
  function copyObject($source, $destination_bucket, $destination_key, $acl) {
    $object = [
      // Source path with bucket eg: bucket/path/to/file.ext
      'CopySource' => $source,
      // Destination bucket
      'Bucket' => $destination_bucket,
      // Destination path/to/file.ext
      'Key' => $destination_key,
      // ACL
      'ACL' => $acl
    ];
    $result = $this->client->copyObject($object);
    return $result;
  }

  /**
   * Delete object
   * @param $bucket
   * @param $key
   * @return \Aws\Result
   */
  function deleteObject($bucket, $key) {
    $result = $this->client->deleteObject(['Bucket' => $bucket, 'Key' => $key]);
    return $result;
  }
}