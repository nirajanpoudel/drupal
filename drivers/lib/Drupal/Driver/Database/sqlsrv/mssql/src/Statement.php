<?php

namespace mssql;

use PDO as PDO;
use PDOException as PDOException;
use PDOStatement as PDOStatement;

/**
 * Turbocharged Statement class to work with MSSQL server.
 */
class Statement extends PDOStatement implements \Countable {
  /**
   * An MD5 signature for the query
   *
   * @var string
   */
  protected $query_signature;

  /**
   * Reference to the Connection
   *
   * @var Connection
   */
  protected $cnn = NULL;

  /**
   * Special PDO options
   *
   * @var array
   */
  protected $options = NULL;

  /**
   * ODBC Codes for integrity constraint violation errors.
   *
   * @var array
   */
  protected $INTEGRITY_VIOLATION_CONSTRAINT_CODES = ['23000'];

  /**
   * ODBC Codes for failed communcation link/dropped connections
   *
   * @var array
   */
  protected $CONNECTION_FAILED_CODES = ['08S01'];

  /**
   * Maximum number a failed query
   * can be retried in case of failure
   * due to integrity constraint violations
   * or dropped connections.
   */
  const RETRY_MAX = 3;

  /**
   * Delay between retries in seconds.
   *
   * This effective delay is the product
   * of this constant and the current retry
   * count.
   */
  const RETRY_DELAY = 0.4;

  /**
   * Return the number of rows.
   *
   * @return int
   */
  public function count() {
    return $this->rowCount();
  }

  /**
   * Set a reference to the connection.
   *
   * @param Connection $cnn
   */
  public function SetConnection(Connection $cnn, array $options = []) {
    // Custom PDO options
    $this->options = array_merge([
         Connection::PDO_RESILIENTRETRY => FALSE,
         Connection::PDO_RETRYONINTEGRITYVIOLATION => FALSE
         ],
         $options
    );
    $this->cnn = $cnn;
  }

  /**
   * Get parameters bound to this statement, useful for debugging purposes.
   *
   * @return string[]
   */
  public function &GetBoundParameters() {
    return $this->boundParams;
  }

  /**
   * @var string[] $boundParams - array of arrays containing values that have been bound to the query as parameters
   */
  protected $boundParams = array();

  /**
   * Overrides the default \PDOStatement method to add the named parameter and it's reference to the array of bound
   * parameters - then accesses and returns parent::bindParam method
   *
   * @param string $param
   * @param mixed $value
   * @param int $datatype
   * @param int $length
   * @param mixed $driverOptions
   * @return bool
   */
  public function bindParam($param, &$value, $datatype = PDO::PARAM_STR, $length = 0, $driverOptions = FALSE) {
    $this->boundParams[$param] = array(
          "value"       => &$value
        , "datatype"    => $datatype
    );

    if (empty($driverOptions)) {
      return parent::bindParam($param, $value, $datatype, $length);
    }
    else {
      return parent::bindParam($param, $value, $datatype, $length, $driverOptions);
    }
  }

  /**
   * Overrides the default \PDOStatement method to add the named parameter and it's value to the array of bound values
   * - then accesses and returns parent::bindValue method
   *
   * @param string $param
   * @param string $value
   * @param int $datatype
   * @return bool
   */
  public function bindValue($param, $value, $datatype = PDO::PARAM_STR) {
    $this->boundParams[$param] = array(
          "value"       => $value
        , "datatype"    => $datatype
    );

    return parent::bindValue($param, $value, $datatype);
  }

  /**
   * Cached version of getColumnMeta().
   *
   * Who knows why this was always profiled as being
   * a memory hog, probably due some problem with
   * the PDO driver.
   *
   * @return mixed
   */
  protected function getColumnMetaCustom() {
    $meta = FALSE;
    if ($cache = $this->cnn->Cache('sqlsrv_meta')->Get($this->query_signature)) {
      return $cache->data;
    }
    // Just some safety to account for some schema
    // changes.
    $meta = [];
    for ($i = 0, $count = $this->columnCount(); $i < $count; $i++) {
      $meta[$i] = $this->getColumnMeta($i);
      $meta[$i]['sqlsrv_type'] = explode(' ', $meta[$i]['sqlsrv:decl_type'])[0];
    }
    $this->cnn->Cache('sqlsrv_meta')->Set($this->query_signature, $meta);
    return $meta;
  }

  /**
   * Make sure that SQL Server types are properly binded to
   * PHP types.
   *
   * Only need when using CLIENT BASED PREFETCH.
   *
   */
  protected function fixColumnBindings() {
    $null = array();
    $meta = $this->getColumnMetaCustom();
    $this->columnNames = array_column($meta, 'name');
    foreach ($meta as $i => $meta) {
      $type = $meta['sqlsrv_type'];
      switch($type) {
        case 'varbinary':
          $null[$i] = NULL;
          $this->bindColumn($i + 1, $null[$i], PDO::PARAM_LOB, 0, PDO::SQLSRV_ENCODING_BINARY);
          break;
        case 'int':
        case 'bit':
        case 'smallint':
        case 'tinyint':
          $null[$i] = NULL;
          $this->bindColumn($i + 1, $null[$i], PDO::PARAM_INT);
          break;
        case 'nvarchar':
        case 'varchar':
          $null[$i] = NULL;
          $this->bindColumn($i + 1, $null[$i], PDO::PARAM_STR, 0, PDO::SQLSRV_ENCODING_UTF8);
          break;
      }
    }
  }

  /**
   * Summary of BindArguments
   *
   * @param PDOStatement $stmt
   * @param array $values
   */
  public function BindArguments(array &$values) {
    foreach ($values as $key => &$value) {
      $this->bindParam($key, $value, PDO::PARAM_STR);
    }
  }

  /**
   * Summary of BindExpressions
   *
   * @param array $values
   * @param array $remove_from
   */
  public function BindExpressions(array &$values, array &$remove_from) {
    foreach ($values as $key => $value) {
      unset($remove_from[$key]);
      if (empty($value['arguments'])) {
        continue;
      }
      if (is_array($value['arguments'])) {
        foreach ($value['arguments'] as $placeholder => $argument) {
          // We assume that an expression will never happen on a BLOB field,
          // which is a fairly safe assumption to make since in most cases
          // it would be an invalid query anyway.
          $this->bindParam($placeholder, $value['arguments'][$placeholder]);
        }
      }
      else {
        $this->bindParam($key, $value['arguments'], PDO::PARAM_STR);
      }
    }
  }

  /**
   * Binds a set of values to a PDO Statement,
   * taking care of properly managing binary data.
   *
   * @param PDOStatement $stmt
   *   PDOStatement to bind the values to
   *
   * @param array $values
   *   Values to bind. It's an array where the keys are column
   *   names and the values what is going to be inserted.
   *
   * @param array $blobs
   *   When sending binary data to the PDO driver, we need to keep
   *   track of the original references to data
   *
   * @param array $ref_prefix
   *   The $ref_holder might be shared between statements, use this
   *   prefix to prevent key colision.
   *
   * @param mixed $placeholder_prefix
   *   Prefix to use for generating the query placeholders.
   *
   * @param mixed $max_placeholder
   *   Placeholder count, if NULL will start with 0.
   *
   */
  public function BindValues(array &$values, array &$blobs, $placeholder_prefix, $columnInformation, &$max_placeholder = NULL, $blob_suffix = NULL) {
    if (empty($max_placeholder)) {
      $max_placeholder = 0;
    }
    foreach ($values as $field_name => &$field_value) {
      $placeholder = $placeholder_prefix . $max_placeholder++;
      $blob_key = $placeholder . $blob_suffix;
      if (isset($columnInformation['blobs'][$field_name])) {
        $blobs[$blob_key] = fopen('php://memory', 'a');
        fwrite($blobs[$blob_key], $field_value);
        rewind($blobs[$blob_key]);
        $this->bindParam($placeholder, $blobs[$blob_key], PDO::PARAM_LOB, 0, PDO::SQLSRV_ENCODING_BINARY);
      }
      else {
        // Even though not a blob, make sure we retain a copy of these values.
        $blobs[$blob_key] = $field_value;
        $this->bindParam($placeholder, $blobs[$blob_key], PDO::PARAM_STR);
      }
    }
  }

  /**
   * Execute a statement.
   *
   * @param array $args
   */
  public function execute($args = NULL) {
    $this->query_signature = md5($this->queryString);
    if ($this->cnn->InDoomedTransaction()) {
      $this->cnn->ThrowDoomedTransactionException();
    }
    $result = NULL;
    try {
      $count = 0;
      while (TRUE) {
        try {
          $count++;
          $result = parent::execute($args);
          break;
        }
        catch (\PDOException $e) {
          // If the maximum retry limit is exceeded
          // throw the exception.
          if ($count > self::RETRY_MAX) {
            throw $e;
          }
          $safe = FALSE;
          if ($this->options[Connection::PDO_RETRYONINTEGRITYVIOLATION] === TRUE
            && in_array((string) $e->getCode(), $this->INTEGRITY_VIOLATION_CONSTRAINT_CODES)) {
            $safe = TRUE;
          }
          if ($this->options[Connection::PDO_RESILIENTRETRY] === TRUE
            && in_array((string) $e->getCode(), $this->CONNECTION_FAILED_CODES)) {
            $safe = TRUE;
          }
          if (!$safe) {
            throw $e;
          }
          else {
            usleep($count * (1000000 * self::RETRY_DELAY));
          }
        }
      }
      if ($result == FALSE) {
        $this->cnn->ThrowPdoException($this, NULL);
      }
      return $result;
    }
    catch (\PDOException $e) {
      $this->cnn->NotifyException($e);
      $this->cnn->ThrowPdoException($this, $e);
      return NULL;
    }
  }

  /**
   * Optimized for common use cases.
   *
   * @param int $key_index
   * @param int $value_index
   *
   * @return array
   */
  public function fetchAllKeyed($key_index = 0, $value_index = 1) {
    // If we are asked for the default behaviour, rely
    // on the PDO as being faster. The result set needs to exactly bee 2 columns.
    if ($key_index == 0 && $value_index == 1 && $this->columnCount() == 2) {
      $this->setFetchMode(PDO::FETCH_KEY_PAIR);
      return $this->fetchAll();
    }
    // We need to do this manually.
    $return = array();
    $this->setFetchMode(PDO::FETCH_NUM);
    foreach ($this as $record) {
      $return[$record[$key_index]] = $record[$value_index];
    }
    return $return;
  }
}