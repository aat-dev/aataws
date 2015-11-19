<?php

use AAT\AWS\DynamoDbConnector;
use Aws\MockHandler;

class DynamoDbConnectorTest extends PHPUnit_Framework_TestCase {

  private $credentials;
  private $mock;

  public function setUp() {
    $this->credentials = array(
      'region' => 'eu-west-1',
      'key' => 'SOMEKEY',
      'secret' => 'SOMESECRET'
    );
    $this->mock = new MockHandler();
  }

  public function testCreateObject() {
    $dynamoDb = new DynamoDbConnector($this->credentials, $this->mock);
    $this->assertNotEmpty($dynamoDb);
  }

}