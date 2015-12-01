<?php

namespace AAT\AWS;

use Aws\DynamoDb\DynamoDbClient,
    Aws\DynamoDb\Exception\DynamoDbException,
    Aws\DynamoDb\Marshaler;

use AAT\AWS\Exception as Exceptions;

class DynamoDbConnector {

  private $client;
  private $marshaler;
  private $pagingLimit;

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
    $this->pagingLimit = 50;
  }

  public function setPagingLimit($limit) {
    $this->pagingLimit = $limit;
  }

  public function getPagingLimit() {
    return $this->pagingLimit;
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
  public function getItems($table, $index, $field, $value, $startkey = array()) {
    try {
      $query = array(
        'TableName' => $table,
        'IndexName' => $index,
        'Limit' => $this->pagingLimit,
        'KeyConditionExpression' => '#field = :attr',
        'ExpressionAttributeNames' => array(
          '#field' => $field
        ),
        'ExpressionAttributeValues' => array(
          ':attr' => array(
            'S' => $value
          )
        )
      );

      if (!empty($startkey)) {
        $query['ExclusiveStartKey'] = $this->marshaler->marshalItem($startkey);
      }

      $response = $this->client->query($query);

      $response = $response->toArray();
      if (isset($response['LastEvaluatedKey'])) {
        $response['LastEvaluatedKey'] = $this->marshaler->unmarshalItem($response['LastEvaluatedKey']);
      }
      foreach ($response['Items'] as $key => $item) {
        $response['Items'][$key] = $this->marshaler->unmarshalItem($item);
      }
      return $response;
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