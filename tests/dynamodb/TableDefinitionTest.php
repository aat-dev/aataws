<?php

use AAT\AWS\TableDefinition;

class TableDefinitionTest extends PHPUnit_Framework_TestCase {

  private $obj;

  public function setUp() {
    $this->obj = new TableDefinition();
  }

  public function testCreateObject() {
    $this->assertNotEmpty($this->obj);
    $this->assertInstanceOf('AAT\AWS\TableDefinition', $this->obj);
  }

  public function testSetTableName() {
    $result = $this->obj->setTableName('test_table');
    $this->assertInstanceOf('AAT\AWS\TableDefinition', $result);
  }

  public function testSetPrimaryKey() {
    $result = $this->obj->setPrimaryKey('test_pk');
    $this->assertInstanceOf('AAT\AWS\TableDefinition', $result);
  }

  public function testSetPrimaryKeyType() {
    $result = $this->obj->setPrimaryKeyType('S');
    $this->assertInstanceOf('AAT\AWS\TableDefinition', $result);
  }

  public function testSetIndex() {
    $result = $this->obj->setIndex('TestIndex-idx' ,'testKey', 'S');
    $this->assertInstanceOf('AAT\AWS\TableDefinition', $result);
  }

  public function testGetTableName() {
    $this->obj->SetTableName('test_table');
    $result = $this->obj->getTableName();
    $this->assertEquals('test_table', $result);
  }

  public function testGetIndex() {
    $this->obj->SetIndex('TestIndex-idx' ,'testKey', 'S');
    $result = $this->obj->getIndex('TestIndex-idx');
    $this->assertArrayHasKey('name', $result);
    $this->assertArrayHasKey('key', $result);
    $this->assertArrayHasKey('type', $result);
    $this->assertEquals('TestIndex-idx', $result['name']);
    $this->assertEquals('testKey', $result['key']);
    $this->assertEquals('S', $result['type']);
  }

  public function testGetIndexOmitType() {
    $this->obj->SetIndex('TestIndex-idx' ,'testKey');
    $result = $this->obj->getIndex('TestIndex-idx');
    $this->assertArrayHasKey('name', $result);
    $this->assertArrayHasKey('key', $result);
    $this->assertArrayHasKey('type', $result);
    $this->assertEquals('TestIndex-idx', $result['name']);
    $this->assertEquals('testKey', $result['key']);
    $this->assertEquals('S', $result['type']);
  }

  public function testGetPrimaryKey() {
    $this->obj->setPrimaryKey('test_pk');
    $result = $this->obj->getPrimaryKey();
    $this->assertEquals('test_pk', $result);
  }

  public function testGetPrimaryKeyType() {
    $this->obj->setPrimaryKeyType('S');
    $result = $this->obj->getPrimaryKeyType();
    $this->assertEquals('S', $result);
  }

}