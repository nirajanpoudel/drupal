<?php
namespace mssql;
use \PDO as PDO;
/**
 * Global settings for the driver.
 */
class ConnectionSettings {
  private $_defaultIsolationLevel;
  private $_defaultDirectQueries;
  private $_defaultStatementCaching;
  private $_statementCachingMode;
  private $_appendStackComments;
  private $_additionalDSN;
  /**
   * Default settings for the dabase driver.
   *
   * @var array
   */
  private static $default_driver_settings = [
        'default_isolation_level' => FALSE,
        'default_direct_queries' => TRUE,
        'default_statement_caching' => FALSE,
        'statement_caching_mode' => 'disabled',
        'append_stack_comments' => FALSE,
        'additional_dsn' => []
      ];
  /**
   * Checks for a valid setting in the list of allowed values.
   *
   * @param mixed $value
   * @param mixed $value
   * @param array $allowed
   */
  private function CheckValid($name, $value, array $allowed) {
    if (!in_array($value, $allowed)) {
      throw new \Exception("Invalid driver setting for $name");
    }
    return $value;
  }
  /**
   * Builds a DriverSettings instance from custom settings. Missing settings are merged
   * from the application settings.
   *
   * @param mixed $configuration
   */
  public static function instanceFromData($configuration = array()) {
    $configuration = array_merge(static::$default_driver_settings, $configuration);
    return new ConnectionSettings($configuration);
  }
  /**
   * Construct an instance of DriverSettings.
   */
  private function __construct($configuration) {
    $this->_defaultIsolationLevel = $this->CheckValid('default_isolation_level', $configuration['default_isolation_level'], array(
        FALSE,
        PDO::SQLSRV_TXN_READ_UNCOMMITTED,
        PDO::SQLSRV_TXN_READ_COMMITTED,
        PDO::SQLSRV_TXN_REPEATABLE_READ,
        PDO::SQLSRV_TXN_SNAPSHOT,
        PDO::SQLSRV_TXN_SERIALIZABLE,
      ));
    $this->_defaultDirectQueries = $this->CheckValid('default_direct_queries', $configuration['default_direct_queries'], array(TRUE, FALSE));
    $this->_defaultStatementCaching = $this->CheckValid('default_statement_caching', $configuration['default_statement_caching'], array(TRUE, FALSE));
    $this->_statementCachingMode = $this->CheckValid('statement_caching_mode', $configuration['statement_caching_mode'], array('disabled', 'on-demand', 'always'));
    $this->_appendStackComments = $this->CheckValid('append_stack_comments', $configuration['append_stack_comments'], array(TRUE, FALSE));
    $this->_additionalDSN = $configuration['additional_dsn'];
  }
  /**
   * Export current driver configuration.
   *
   * @return array
   */
  public function exportConfiguration() {
    return array(
        'default_isolation_level' => $this->GetDefaultIsolationLevel(),
        'default_direct_queries' => $this->GetDefaultDirectQueries(),
        'statement_caching_mode' => $this->GetStatementCachingMode(),
        'append_stack_comments' => $this->GetAppendCallstackComment(),
        'default_statement_caching' => $this->GetDeafultStatementCaching(),
        'additional_dsn' => $this->GetAdditionalDSN(),
      );
  }
  /**
   * Isolation level used for implicit transactions.
   */
  public function GetAdditionalDSN() {
    return $this->_additionalDSN;
  }
  /**
   * Isolation level used for implicit transactions.
   */
  public function GetDefaultIsolationLevel() {
    return $this->_defaultIsolationLevel;
  }
  /**
   * PDO Constant names do not match 1-to-1 the transaction names that
   * need to be used in SQL.
   *
   * @return mixed
   */
  public function GetDefaultTransactionIsolationLevelInStatement() {
    return str_replace('_', ' ', $this->GetDefaultIsolationLevel());
  }
  /**
   * Default query preprocess.
   *
   * @return mixed
   */
  public function GetDeafultStatementCaching() {
    return $this->_defaultStatementCaching;
  }
  /**
   * Wether to run all statements in direct query mode by default.
   */
  public function GetDefaultDirectQueries() {
    return $this->_defaultDirectQueries;
  }
  /**
   * Enable appending of PHP stack as query comments.
   */
  public function GetAppendCallstackComment() {
    return $this->_appendStackComments;
  }
  /**
   * Experimental statement caching for PDO prepared statement
   * reuse.
   *
   * 'disabled' => Never use statement caching.
   * 'on-demand' => Only use statement caching when implicitly set in a Context.
   * 'always' => Always use statement caching.
   *
   */
  public function GetStatementCachingMode() {
    return $this->_statementCachingMode;
  }

  /**
   * Build the connection string.
   * 
   * @param array $options
   * 
   * @return string
   */
  public function buildDSN(array $options) {
    // Merge the original options with the
    // aditional DSN settings.
    $options = $options + $this->GetAdditionalDSN();
    $dsn = 'sqlsrv:';
    foreach ($options as $key => $value) {
      $dsn .= (empty($key) ? '' : "{$key}=") . $value . ';';
    }
    return $dsn;
  }
}