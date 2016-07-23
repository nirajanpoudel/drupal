<?php
namespace mssql;
use PDO;
use PDOException;
class Connection extends PDO {
  /**
   * Use this when preparing a statement to
   * retry operations that fail with integrity
   * constraint violations. Useful when
   * using MERGE statements - than can fail
   * on high concurrency scenarios.
   */
  const PDO_RETRYONINTEGRITYVIOLATION = 'PDO_RETRYONINTEGRITYVIOLATION';
  /**
   * Some environments such as Azure require retry logic
   * at the statement level. Use this to enable it.
   */
  const PDO_RESILIENTRETRY = 'PDO_RESILIENTRETRY';
  /**
   * @var Scheme
   */
  private $scheme = NULL;
  /**
   * @var Component\CacheFactoryInterface
   */
  private $cache = NULL;
  /**
   * If the transaction is doomed.
   *
   * @var bool
   */
  private $doomed_transaction = FALSE;
  /**
   * The original exception that doomed the transaction.
   * 
   * @var \Exception
   */
  private $doomed_transaction_exception = NULL;
  /**
   * If we are currently inside a transaction.
   *
   * @var bool
   */
  private $in_transaction = FALSE;
  public function __construct($dsn, $username = NULL, $password = NULL, array $driver_options = array(), Component\CacheFactoryInterface $cache = NULL) {
    if (empty($cache)) {
      $this->cache = new Component\CacheFactoryDefault(md5(implode(':', [$dsn, $username, $password])));
    }
    else {
      $this->cache = $cache;
    }
    parent::__construct($dsn, $username, $password, $driver_options);
    $this->scheme = new Scheme($this);
  }
  /**
   * @return Scheme
   */
  public function Scheme() {
    return $this->scheme;
  }
  /**
   * @return Component\CacheInterface
   */
  public function Cache($bin = 'cache') {
    return $this->cache->get($bin);
  }
  /**
   * If we are in a transaction and the transaction is doomed.
   */
  public function InDoomedTransaction() {
    return $this->doomed_transaction;
  }
  /**
   * {@inheritdoc}
   */
  public function prepare($query, $options = []) {
    // Remove our custom prepare options, otherwise the PDO will
    // crash.
    $custom = [self::PDO_RESILIENTRETRY, self::PDO_RETRYONINTEGRITYVIOLATION];
    $pdo_options = array_diff_key($options, array_flip($custom));
    /** @var Statement */
    $statement  = parent::prepare($query, $pdo_options);
    $statement->SetConnection($this, $options);
    return $statement;
  }
  /**
   * {@inhertidoc}
   */
  public function exec($statement) {
    try {
      return parent::exec($statement);
    }
    catch (\PDOException $e) {
      $this->NotifyException($e);
      throw $e;
    }
  }
  /**
   * {@inheritdoc}
   */
  public function query($statement, $fetch_mode = NULL, $p1 = NULL, $p2 = NULL) {
    // By overriding this we are just making sure that we are able to INTERCEPT
    // any exception that might happen during a transaction.
    try {
      if (empty($fetch_mode)) {
        return parent::query($statement);
      }
      switch ($fetch_mode) {
        case PDO::FETCH_COLUMN:
          return parent::query($statement, $fetch_mode, $p1);
        case PDO::FETCH_CLASS:
          return parent::query($statement, $fetch_mode, $p1, $p2);
        case PDO::FETCH_INTO:
          return parent::query($statement, $fetch_mode, $p1, $p2);
        default:
          throw new \Exception("query() call not supported. Second argument needs to be one of PDO::FETCH_COLUMN | PDO::FETCH_CLASS | PDO::FETCH_INTO. Use query_execute() instead.");
      }
    }
    catch (\PDOException $e) {
      $this->NotifyException($e);
      throw $e;
    }
  }
  protected function defaultOptions() {
    return array(
      'target' => 'default',
      'fetch' => \PDO::FETCH_OBJ,
      'throw_exception' => TRUE,
      'allow_delimiter_in_query' => FALSE,
    );
  }
  /**
   * Custom function to easy querying data without needing to prepare.
   *
   * @param mixed $query
   * @param array $args
   * @param mixed $options
   * @throws PDOException
   * @return mixed
   */
  public function query_execute($query, array $args = array(), $options = array()) {
    try {
      // Make sure we are not preparing statements.
      $options[PDO::SQLSRV_ATTR_DIRECT_QUERY] = TRUE;
      /** @var Statement */
      $stmt = $this->prepare($query, $options);
      $stmt->execute($args);
      return $stmt;
    }
    catch (\PDOException $e) {
      $this->NotifyException($e);
      throw $e;
    }
  }
  /**
   * {@inheritdoc}
   */
  public function rollBack() {
    $this->in_transaction = FALSE;
    $this->doomed_transaction = FALSE;
    return parent::rollBack();
  }
  /**
   * {@inheritdoc}
   */
  public function beginTransaction() {
    parent::beginTransaction();
    $this->in_transaction = TRUE;
  }
  /**
   * {@inheritdoc}
   */
  public function commit() {
    if ($this->doomed_transaction) {
      // We are about to throw an Exception, this is the last word of warning...
      // so it is safe to release the lock from the connection now...
      $this->doomed_transaction = FALSE;
      $this->ThrowDoomedTransactionException();
    }
    $this->in_transaction = FALSE;
    return parent::commit();
  }
  /**
   * PDO Exception codes that we know will not doom
   * the current transaction.
   *
   * @var array
   */
  protected $allowed_pdo_exception_codes = array('42S02' => TRUE);
  /**
   * Only to be used by statements to notify of a PDO exception.
   *
   * @param \PDOException $e
   */
  public function NotifyException(\PDOException $e) {
    // Protection against issue of PDO driver.
    if ($this->in_transaction) {
      // Some PDO exceptions are "safe" and will not doom the
      // transaction.
      if (!isset($this->allowed_pdo_exception_codes[$e->getCode()])) {
        $this->doomed_transaction = TRUE;
        $this->doomed_transaction_exception = $e;
      }
    }
  }
  /**
   * @see https://github.com/Azure/msphpsql/issues/50
   *
   * @throws PDOException
   */
  public function ThrowDoomedTransactionException() {
    throw new DoomedTransactionException("Msg 3930, Level 16, State 1, Line 21\r\nThe current transaction cannot be committed and cannot support operations that write to the log file. Roll back the transaction.", 0, $this->doomed_transaction_exception);
  }
  /**
   * Get the current callstack as a comment that can be appended to a query.
   *
   * @param string $application_root
   *   Application root to remove from the callstack dump.
   *
   * @param array $extras
   *   Any application specific information that needs to be dumped.
   *
   * @return string
   */
  public function GetCallstackAsComment($application_root, array $extras = array()) {
    $trim = strlen($application_root);
    $trace = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS);
    // Remove last items.
    $trace = array_splice($trace, 2);
    $comment = PHP_EOL . PHP_EOL;
    foreach ($extras as $extra) {
      $comment .= $extra . PHP_EOL;
    }
    $uri = (isset($_SERVER['REQUEST_URI']) ? $_SERVER['REQUEST_URI'] : 'none') ;
    $uri = preg_replace("/[^a-zA-Z0-9]/i", "_", $uri);
    $comment .= '-- url:' . $uri . PHP_EOL;
    foreach ($trace as $t) {
      $function = isset($t['function']) ? $t['function'] : '';
      $file = '';
      if(isset($t['file'])) {
        $len = strlen($t['file']);
        if ($len > $trim) {
          $file = substr($t['file'], $trim, $len - $trim) . " [{$t['line']}]";
        }
      }
      $comment .= '-- ' . str_pad($function, 35) . '  ' . $file . PHP_EOL;
    }
    $comment .= PHP_EOL;
    return $comment;
  }
  /**
   * This is a helper method to rethrow an Exception if the execution
   * of a PDOStatement fails.
   *
   * Sometimes, as a result of a PDO Statement execution error
   * the error itself will be found in the connection and no in the statement.
   *
   */
  public function ThrowPdoException(Statement &$statement = NULL, \PDOException $e = NULL) {
    // This is what a SQL Server PDO "no error" looks like.
    $null_error = array(0 => '00000', 1 => NULL, 2 => NULL);
    $error_info_connection = $this->errorInfo();
    if ($error_info_connection == $null_error && $e !== NULL) {
      throw $e;
    }
    $error_info_statement =  !empty($statement) ? $statement->errorInfo() : $null_error;
    // TODO: Concatenate error information when both connection
    // and statement error info are valid.
    // We rebuild a message formatted in the same way as PDO.
    $error_info = ($error_info_connection === $null_error) ? $error_info_statement : $error_info_connection;
    $code = $e && is_numeric($e->getCode()) ? $e->getCode() : 0;
    $exception = new PDOException("SQLSTATE[" . $error_info[0] . "]: General error " . $error_info[1] . ": " . $error_info[2], $code, $e);
    $exception->errorInfo = $error_info;
    unset($statement);
    throw $exception;
  }
  /**
   * Generate a sequence
   *
   * @param int $existing
   *   The sequence value must be greater than this value.
   * @param string $name
   *   Name of the sequence.
   * @throws \Exception
   * @return mixed
   */
  public function nextId($min = 0, $name = 'default') {

    if (!is_int($min)) {
      throw new \InvalidArgumentException("Minimum id value must be an integer: $min");
    }

    // The sequence name must be unique for this installation.
    $sequence_name = 'seq_' . $name;

    try {
      $next_id = $this->query_execute("SELECT NEXT VALUE FOR $sequence_name")->fetchField();
    }
    catch (\Exception $e) {
      // Create the sequence starting at $min + 2,
      // because $min + 1 is already being used in this request.
      $start = $min + 2;
      // The "first id" is the same as the minimum possible
      // value, because the minimum for MSSQL is inclusive, while
      // this function's definition $min is exclusive (must
      // be greater.
      $next_id = $min + 1;
      $this->query_execute("CREATE SEQUENCE $sequence_name START WITH $start INCREMENT BY 1 MINVALUE $next_id");
    }

    // If the retrieve id is smaller or equal to de existent,
    // restart the sequence to the provided number.
    if ($next_id <= $min) {
      $min++;
      $this->query_execute("ALTER SEQUENCE $sequence_name RESTART WITH $min");
      $next_id = $this->query_execute("SELECT NEXT VALUE FOR $sequence_name")->fetchColumn();
    }

    return $next_id;
  }
}
