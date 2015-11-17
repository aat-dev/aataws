<?php

use AAT\AWS\DynamoDbConnector;

class DynamoDbConnectorTest extends PHPUnit_Framework_TestCase {

  public function testCreateObject() {
    $dynamoDb = new DynamoDbConnector(array('secret' => '123', 'key' => '456', 'region' => 'eu-west-1'));
    $this->assertNotEmpty($dynamoDb);

  }

}