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
      throw new Exceptions\FatalException($e->getMessage());
    }
  }

  /**
   * Get items by querying DynamoDb.
   *
   * @param $table
   * @param $index
   * @param $field
   * @param $value
   * @throws Exception\FatalException
   */
  public function getItems($table, $index, $field, $value) {
    try {
      $response = $this->client->query(
        [
          'TableName' => $table,
          'IndexName' => $index,
          'KeyConditionExpression' => '#field = :attr',
          'ExpressionAttributeNames' => ['#field' => $field],
          'ExpressionAttributeValues' => [
            ':attr' => [
              'S' => $value
              ]
            ]
        ]
      );

      $response = $response->toArray();
      $items = array();
      foreach ($response['Items'] as $item) {
        $items[] = $this->marshaler->unmarshalItem($item);
      }

      return $items;
    }
    catch (DynamoDbException $e) {
      throw new Exceptions\FatalException($e->getMessage());
    }
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