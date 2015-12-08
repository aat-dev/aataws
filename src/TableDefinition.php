<?php

namespace AAT\AWS;

use AAT\AWS\Exception as Exceptions;

class TableDefinition {

  private $table;
  private $indexes;
  private $pk;
  private $pktype;

  public function __construct($definition = FALSE) {
    if ($definition) {
      $def = json_decode($definition);
      $this->table = $def->table;
      $this->indexes = $def->indexes;
    }
  }

  public function setTableName($tablename) {
    $this->table = $tablename;
    return $this;
  }

  public function setPrimaryKey($pk) {
    $this->pk = $pk;
    return $this;
  }

  public function setPrimaryKeyType($type) {
    $this->pktype = $type;
    return $this;
  }

  public function setIndex($name, $key, $type = FALSE) {
    $this->indexes[$name] = array(
      'name' => $name,
      'key' => $key,
      'type' => $type ? $type : 'S'
    );
    return $this;
  }

  public function getTableName() {
    return $this->table;
  }

  public function getIndex($name) {
    return $this->indexes[$name];
  }

  public function getPrimaryKey() {
    return $this->pk;
  }

  public function getPrimaryKeyType() {
    return $this->pktype;
  }

}