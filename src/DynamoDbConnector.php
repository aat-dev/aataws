<?php

namespace AAT\AWS;

use Aws\DynamoDb\DynamoDbClient,
    Aws\DynamoDb\Exception\DynamoDbException,
    Aws\DynamoDb\Marshaler;

use AAT\AWS\Exception as Exceptions;

class DynamoDbConnector {

  private $client;
  private $marshaler;

  /**
   * Dynamo connector constructor.
   */
  public function __construct($credentials, $handler = NULL) {
    $config = array(
      'region' => $credentials['region'],
      'version' => 'latest',
      'credentials' => array(
        'key' => $credentials['key'],
        'secret' => $credentials['secret']
      )
    );
    if ($handler) {
      $config['handler'] = $handler;
    }
    $this->client = new DynamoDbClient($config);
    $this->marshaler = new Marshaler();
  }

  /**
   * Create an item.
   *
   * @param $table
   * @param string $json
   */
  public function createItem($table, $json) {
    try {
      $response = $this->client->putItem(
        [
          'TableName' => $table,
          'Item' => $this->marshaler->marshalJson($json)
        ]
      );
      $response = $response->toArray();
      if ($response['@metadata']['statusCode'] == 200) {
        return TRUE;
      }
    } catch (DynamoDbException $e) {
      if ($e->getAwsErrorType() == 'client') {
        throw new Exceptions\FatalException($e->getMessage());
      } else {
        throw new Exceptions\SystemException($e->getMessage());
      }
    }

    return FALSE;
  }

  /**
   * Get an item.
   *
   * @param $table
   * @param $key
   * @param $value
   * @return bool
   */
  public function getItem($table, $key, $value) {
    try {
      $response = $this->client->getItem(
        [
          'TableName' => $table,
          'ConsistentRead' => TRUE,
          'Key' => [
            $key => [
              'S' => $value,
            ]
          ]
        ]
      );
      return $this->marshaler->unmarshalItem($response['Item']);
    }
    catch (DynamoDbException $e) {
      $this->reportErr($e);
    }
    return FALSE;
  }

  /**
   * Scan a table.
   *
   * @param $table
   * @param $key
   * @return bool
   */
  public function scan($table, $key) {
    try {
      $response = $this->client->scan(
        [
          'ConsistentRead' => TRUE,
          'IndexName' => $key,
          'TableName' => $table
        ]
      );
      return $response['Items'];
    }
    catch (DynamoDbException $e) {
      $this->reportErr($e);
    }
    return FALSE;
  }

  /**
   * return errors
   *
   * @param Exception $exception
   */
  private function reportErr(Exception $exception) {
    return $exception->getMessage();
  }

  /**
   * Delete an item.
   *
   * @param $table
   * @param $key
   * @param $value
   */
  public function deleteItem($table, $key, $value) {
    $response = $this->client->deleteItem(
      [
        'TableNmae' => $table,
        'Key' => [
          $key => [
            'S' => $value
          ]
        ]
      ]
    );
  }

}