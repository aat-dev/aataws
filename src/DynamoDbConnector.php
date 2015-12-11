<?php

namespace AAT\AWS;

use Aws\DynamoDb\DynamoDbClient,
    Aws\DynamoDb\Exception\DynamoDbException,
    Aws\DynamoDb\Marshaler;

use AAT\AWS\Exception as Exceptions;
use AAT\AWS\TableDefinition;

class DynamoDbConnector {

  private $client;
  private $marshaler;
  private $pagingLimit;
  private $tableDef;

  /**
   * Dynamo connector constructor.
   */
  public function __construct($credentials, TableDefinition $tableDefinition, $handler = NULL) {
    $config = array(
      'region' => $credentials['region'],
      'version' => 'latest',
      'credentials' => array(
        'key' => $credentials['key'],
        'secret' => $credentials['secret']
      )
    );
    $this->tableDef = $tableDefinition;
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
  public function createItem($json) {
    try {
      $response = $this->client->putItem(
        [
          'TableName' => $this->tableDef->getTableName(),
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
  public function getItem($value) {
    try {

      $config = array(
        'TableName' => $this->tableDef->getTableName(),
        'ConsistentRead' => TRUE,
        'Key' => array(
          $this->tableDef->getPrimaryKey() => array(
            $this->tableDef->getPrimaryKeyType() => $value
          )
        )
      );

      $response = $this->client->getItem($config);
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
  public function getItems($index, $value, $conditions = array(), $startkey = array()) {

    try {
      $index = $this->tableDef->getIndex($index);
      $expressionAttributeNames = array(
        '#key_field_placeholder' => $index['key']
      );
      $expressionAttributeValues = array(
        ':key_value_placeholder' => array(
          $index['type'] => $value
        )
      );

      $query = array(
        'TableName' => $this->tableDef->getTableName(),
        'IndexName' => $index['name'],
        'Limit' => $this->pagingLimit,
        'KeyConditionExpression' => '#key_field_placeholder = :key_value_placeholder'
      );

      if (!empty($conditions)) {
        $filterExpressions = array();

        foreach ($conditions as $key => $condition) {

          // Set up defaults
          $comp = isset($condition['comparator']) ? $condition['comparator'] : '=';
          $dataType = (isset($condition['datatype'])) ? $condition['datatype'] : 'S';

          $key_placeholder = '#filter_field_placeholder_' . $key;

          // Do FIELD IN (VALUE_1, VALUE_2, VALUE_3) queries
          if (is_array($condition['value'])) {
            $val_placeholders = array();
            foreach ($condition['value'] as $i => $val) {
              $val_placeholder = ':filter_value_placeholder_' . $key . '_' . $i;
              $val_placeholders[] = $val_placeholder;
              $expressionAttributeValues[$val_placeholder] = array(
                $dataType => $val
              );
            }
            $filterExpressions[] = $key_placeholder . ' IN (' . implode(', ', $val_placeholders) . ')';
          }
          else {
            $val_placeholder = ':filter_value_placeholder_' . $key;
            $filterExpressions[] = $key_placeholder . ' ' . $comp . ' ' . $val_placeholder;
            $expressionAttributeValues[$val_placeholder] = array(
              $dataType => $condition['value']
            );
          }

          $expressionAttributeNames[$key_placeholder] = $condition['field'];
        }

        $query['FilterExpression'] = implode(' AND ', $filterExpressions);
      }

      $query['ExpressionAttributeNames'] = $expressionAttributeNames;
      $query['ExpressionAttributeValues'] = $expressionAttributeValues;

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