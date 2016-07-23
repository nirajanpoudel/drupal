<?php

namespace mssql;

use mssql\Settings\ConstraintTypes;
use mssql\Settings\RecoveryModel;

class Scheme {

  /**
   * Maximum length of a comment in SQL Server.
   */
  const COMMENT_MAX_BYTES = 7500;

  /**
   * Maximum index size when an XML index is present.
   */
  const INDEX_MAX_SIZE_WITH_XML = 128;

  /**
   * Connection.
   *
   * @var Connection
   */
  private $cnn = NULL;

  public function __construct(Connection $cnn) {
    $this->cnn = $cnn;
  }

  /**
   * Get the SQL expression for a default value that can be embedded directly
   * into a query.
   *
   * @param string $sqlsr_type
   *   Sql server type: nvarchar, varbinary, char, ntext, etc.
   * @param mixed $default
   *   The default value.
   */
  public function DefaultValueExpression($sqlsr_type, $default) {
    // The actual expression depends on the target data type as it might require conversions.
    $result = is_string($default) ? $this->cnn->quote($default) : $default;
    if (Utils::GetMSSQLType($sqlsr_type) == 'varbinary') {
      $default = $this->cnn->quote($default);
      $result = "CONVERT({$sqlsr_type}, {$default})";
    }
    return $result;
  }

  /**
   * Verify if a in index exists in the database.
   *
   * @param string $table
   *   Name of the table.
   * @param string $index
   *   Name of the index.
   * @return bool
   */
  public function IndexExists($table, $index) {
    return (bool) $this->cnn->query_execute('SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(:table) AND name = :name', array(
      ':table' => $table,
      ':name' => $index
    ))->fetchField();
  }

  /**
   * Check if a constraint exists.
   *
   * @param string $table
   * @param ConstraintTypes $type
   * @return bool
   */
  public function ConstraintExists($name, ConstraintTypes $type) {
    return (bool) $this->cnn->query_execute("SELECT CASE WHEN OBJECT_ID(:name, :type) IS NULL THEN 0 ELSE 1 END", array(
      ':type' => $type->__toString(),
      ':name' => "dbo.[$name]"
    ))->fetchField();
  }

  /**
   * Drop an index, nothing to to if the index does not exists.
   *
   * @param string $table
   *   Name of the table.
   * @param string $index
   *   Name of the index.
   * @return void
   */
  public function IndexDrop($table, $index) {
    if (!$this->IndexExists($table, $index)) {
      // Nothing to do....
      return;
    }

    $this->cnn->query_execute("DROP INDEX {$index} ON {$table}");
  }

  /**
   * Drop a column form a table.
   *
   * @param string $table
   *   Table name.
   * @param string $column
   *   Colum name.
   *
   * @return bool
   */
  public function FieldExists($table, $column) {
    return $this->cnn
        ->query_execute("SELECT 1 FROM INFORMATION_SCHEMA.columns WHERE table_name = '{$table}' AND column_name = '{$column}'")
        ->fetchField() !== FALSE;
  }

  /**
   * Drop a statistic
   *
   * @param string $table
   *   Table name.
   * @param string $statistics
   *   Statistics name.
   */
  public function StatisticsDrop($table, $statistics) {
    $this->cnn->query_execute("DROP STATISTICS {$table}.{$statistics}");
  }

  /**
   * Check if a statistic already exists.
   *
   * @param string $table
   * @param string $statistics
   * @return bool
   */
  public function StatisticsExists($table, $statistics) {

    $query = <<<EOF
SELECT stat.name AS Statistics,
 OBJECT_NAME(stat.object_id) AS Object,
 COL_NAME(scol.object_id, scol.column_id) AS Column
FROM sys.stats AS stat (NOLOCK) Join sys.stats_columns AS scol (NOLOCK)
 ON stat.stats_id = scol.stats_id AND stat.object_id = scol.object_id
 INNER JOIN sys.tables AS tab (NOLOCK) on tab.object_id = stat.object_id
WHERE OBJECT_NAME(stat.object_id) = :table AND
stat.name = :statistics
EOF;

    return $this->cnn
      ->query_execute($query, array(
          ':table' => $table,
          ':statistics' => $statistics,
        ))
      ->fetchField() !== FALSE;
  }

  /**
   * Check if a trigger exists.
   *
   * @param string $name
   */
  public function TriggerExists($name) {
    return $this->cnn
      ->query_execute("SELECT 1 FROM sys.triggers WHERE name = :name", array(':name' =>$name))
      ->fetchField() !== FALSE;
  }

  /**
   * Drop a triggerr.
   *
   * @param string $name
   */
  public function TriggeDrop($name) {
    $this->cnn->query_execute("DROP TRGGER {$name}");
  }

  /**
   * Check if a view exists.
   *
   * @param string $name
   */
  public function ViewExists($name) {
    return $this->cnn
    ->query_execute("SELECT 1 FROM INFORMATION_SCHEMA.views WHERE table_name = :name", array(':name' =>$name))
    ->fetchField() !== FALSE;
  }

  /**
   * Drop a View.
   *
   * @param string $name
   */
  public function ViewDrop($name) {
    $this->cnn->query_execute("DROP VIEW {$name}");
  }

  /**
   * Drop a Function.
   *
   * @param string $name
   */
  public function FunctionDrop($name) {
    $this->cnn->query_execute("DROP FUNCTION {$name}");
  }

  /**
   * Check if a table already exists.
   *
   * @param string $table
   *   Name of the table.
   *
   * @return boolean
   *   True if the table exists, false otherwise.
   */
  public function TableExists($table, $refresh = FALSE) {

    $bin = $this->cnn->Cache('sqlsrv-table-exists');

    if (!$bin->Get('@@preloaded')) {
      foreach ($this->cnn->query_execute("SELECT table_name FROM INFORMATION_SCHEMA.tables") as $t) {
        $bin->Set($t->table_name, TRUE);
      }
      $bin->Set('@@preloaded', TRUE);
    }

    if(!$refresh && $cache = $bin->Get($table)) {
      return $cache->data;
    }

    // Temporary tables and regular tables cannot be verified in the same way.
    $query = NULL;

    if ($table[0] == '#') {
      $table .= '%';
      $query = "SELECT 1 FROM tempdb.sys.tables WHERE name like :table";
    }
    else {
      $query = "SELECT 1 FROM INFORMATION_SCHEMA.tables WHERE table_name = :table";
    }

    $exists = $this->cnn->query_execute($query, [':table' => $table])->fetchField() !== FALSE;

    if ($exists) {
      $bin->Set($table, $exists);
    }

    return $exists;
  }

  /**
   * Drop a table.
   *
   * @param string $table
   *   Name of the table to drop.
   *
   * @return bool
   */
  public function TableDrop($table) {
    if (!$this->TableExists($table, TRUE)) {
      return FALSE;
    }
    $this->cnn->query_execute("DROP TABLE [{$table}]");
    $this->cnn->Cache('sqlsrv-table-exists')->Clear($table);
    return TRUE;
  }

  /**
   * Check if a table already has an XML index.
   *
   * @param string $table
   *   Name of the table.
   */
  public function TableHasXmlIndex($table) {
    $info = $this->TableDetailsGet($table);
    if (isset($info['indexes']) && is_array($info['indexes'])) {
      foreach ($info['indexes'] as $name => $index) {
        if (strcasecmp($index['type_desc'], 'XML') === 0) {
          return $name;
        }
      }
    }
    return FALSE;
  }

  /**
   * Return active default Schema.
   */
  public function GetDefaultSchema() {
    if ($cache = $this->cnn->Cache('sqlsrv-engine')->Get('default_schema')) {
      return $cache->data;
    }
    $result = $this->cnn->query_execute("SELECT SCHEMA_NAME()")->fetchField();
    $this->cnn->Cache('sqlsrv-engine')->Set('default_schema', $result);
    return $result;
  }

  /**
   * Remove comments from an SQL statement.
   * @see http://stackoverflow.com/questions/9690448/regular-expression-to-remove-comments-from-sql-statement
   *
   * @param mixed $sql
   *  SQL statement to remove the comments from.
   *
   * @param mixed $comments
   *  Comments removed from the statement
   *
   * @return string
   */
  public function removeSQLComments($sql, &$comments = NULL) {
    $sqlComments = '@(([\'"]).*?[^\\\]\2)|((?:\#|--).*?$|/\*(?:[^/*]|/(?!\*)|\*(?!/)|(?R))*\*\/)\s*|(?<=;)\s+@ms';
    /* Commented version
    $sqlComments = '@
    (([\'"]).*?[^\\\]\2) # $1 : Skip single & double quoted expressions
    |(                   # $3 : Match comments
    (?:\#|--).*?$    # - Single line comments
    |                # - Multi line (nested) comments
    /\*             #   . comment open marker
    (?: [^/*]    #   . non comment-marker characters
    |/(?!\*) #   . ! not a comment open
    |\*(?!/) #   . ! not a comment close
    |(?R)    #   . recursive case
    )*           #   . repeat eventually
    \*\/             #   . comment close marker
    )\s*                 # Trim after comments
    |(?<=;)\s+           # Trim after semi-colon
    @msx';
     */
    $uncommentedSQL = trim(preg_replace($sqlComments, '$1', $sql));
    if (is_array($comments)) {
      preg_match_all($sqlComments, $sql, $comments);
      $comments = array_filter($comments[ 3 ]);
    }
    return $uncommentedSQL;
  }

  /**
   * Current configuration for the connection.
   *
   * @return Scheme\UserOptions
   */
  public function UserOptions() {
    if ($cache = $this->cnn->Cache('sqlsrv-engine')->Get('UserOptions')) {
      return $cache->data;
    }
    $data = Scheme\UserOptions::Get($this->cnn);
    $this->cnn->Cache('sqlsrv-engine')->Set('UserOptions', $data);
    return $data;
  }

  /**
   * Get the description property of a table or column.
   *
   * @param string $table
   *
   * @param string $column
   *
   * @return string
   */
  public function CommentGet($table, $column = NULL) {

    $arguments = array('MS_Description','Schema', $this->GetDefaultSchema(), 'Table', $table);
    if (!empty($column)) {
      $arguments[] = 'column';
      $arguments[] = $column;
    }

    $args = call_user_func_array(Utils::class . '::GetExtendedProperty', $arguments);

    $extended_property = $this->cnn->query_execute($args['query'], $args['args'])->fetchAssoc();
    return $extended_property['value'];
  }

  /**
   * Return the SQL statement to create or update a description.
   */
  public function CommentCreate($value, $table = NULL, $column = NULL) {

    // Inside the same transaction, you won't be able to read uncommited extended properties
    // leading to SQL Exception if calling sp_addextendedproperty twice on same object.
    static $columns = array();

    $schema = $this->GetDefaultSchema();
    $name = 'MS_Description';

    // Determine if a value exists for this database object.
    $key = $schema . '.' .  $table . '.' . $column;
    if(isset($columns[$key]) && $this->cnn->inTransaction()) {
      $result = $columns[$key];
    }
    else {
      $result = $this->CommentGet($table, $column);
    }

    $columns[$key] = $value;

    // Only continue if the new value is different from the existing value.
    $sql = '';
    if ($result !== $value) {
      if ($value == '') {
        $sp = "sp_dropextendedproperty";
        $sql = "EXEC " . $sp . " @name=N'" . $name;
      }
      else {
        if ($result != '') {
          $sp = "sp_updateextendedproperty";
        }
        else {
          $sp = "sp_addextendedproperty";
        }
        $sql = "EXEC " . $sp . " @name=N'" . $name . "', @value=" . $value . "";
      }
      if (isset($schema)) {
        $sql .= ",@level0type = N'Schema', @level0name = '". $schema ."'";
        if (isset($table_prefixed)) {
          $sql .= ",@level1type = N'Table', @level1name = '". $table_prefixed ."'";
          if ($column !== NULL) {
            $sql .= ",@level2type = N'Column', @level2name = '". $column ."'";
          }
        }
      }
    }

    return $sql;
  }

  /**
   * Invalidate cache for TableDetailsGet.
   *
   * @param string $table
   */
  public function TableDetailsInvalidate($table) {
    $this->cnn->Cache('sqlsrv-tabledetails')->Clear($table);
  }

  /**
  /**
   * Database introspection: fetch technical information about a table.
   *
   *   An array with the following structure:
   *   - blobs[]: Array of column names that should be treated as blobs in this table.
   *   - identities[]: Array of column names that are identities in this table.
   *   - identity: The name of the identity column
   *   - columns[]: An array of specification details for the columns
   *      - name: Column name.
   *      - max_length: Maximum length.
   *      - precision: Precision.
   *      - collation_name: Collation.
   *      - is_nullable: Is nullable.
   *      - is_ansi_padded: Is ANSI padded.
   *      - is_identity: Is identity.
   *      - definition: If a computed column, the computation formulae.
   *      - default_value: Default value for the column (if any).
   *
   * @param string $table
   *
   * @return array
   */
  public function TableDetailsGet($table) {
    if ($cache = $this->cnn->Cache('sqlsrv-tabledetails')->Get($table)) {
      // The correctness of this data is so important for the database layer
      // to work, that we double check that it is - at least - valid.
      if (isset($cache->data['columns']) && !empty($cache->data['columns'])) {
        return $cache->data;
      }
    }

    // We could adapt the current code to support temporary table introspection, but
    // for now this is not supported.
    if ($table[0] == '#') {
      throw new \Exception('Temporary table introspection is not supported.');
    }

    $schema = $this->GetDefaultSchema();

    // Initialize the information array.
    $info = [
     'identity' => NULL,
     'identities' => [],
     'columns' => [],
     'columns_clean' => []
    ];

    // Don't use {} around information_schema.columns table.
    $result = $this->cnn->query_execute("SELECT sysc.name, sysc.max_length, sysc.precision, sysc.collation_name,
                    sysc.is_nullable, sysc.is_ansi_padded, sysc.is_identity, sysc.is_computed, TYPE_NAME(sysc.user_type_id) as type,
                    syscc.definition,
                    sm.[text] as default_value
                    FROM sys.columns AS sysc
                    INNER JOIN sys.syscolumns AS sysc2 ON sysc.object_id = sysc2.id and sysc.name = sysc2.name
                    LEFT JOIN sys.computed_columns AS syscc ON sysc.object_id = syscc.object_id AND sysc.name = syscc.name
                    LEFT JOIN sys.syscomments sm ON sm.id = sysc2.cdefault
                    WHERE sysc.object_id = OBJECT_ID(:table)
                    ",
                  array(':table' => $schema . '.' . $table));

    foreach ($result as $column) {
      if ($column->type == 'varbinary') {
        $info['blobs'][$column->name] = TRUE;
      }

      // Add the complete SQL Server type with length
      $column->sqlsrv_type = $column->type;
      if ($this->IsVariableLengthType($column->type)) {
        if ($column->max_length == -1) {
          $column->sqlsrv_type .= "(max)";
        }
        else {
          $column->sqlsrv_type .= "($column->max_length)";
        }
      }

      $info['columns'][$column->name] = (array) $column;
      // Provide a clean list of columns that excludes the ones internally created by the
      // database driver.
      if (!(isset($column->name[1]) && substr($column->name, 0, 2) == "__")) {
        $info['columns_clean'][$column->name] = (array) $column;
      }
      if ($column->is_identity) {
        $info['identities'][$column->name] = $column->name;
        $info['identity'] = $column->name;
      }
    }

    // We should have some column data here, otherwise there is a
    // chance that the table does not exist.
    if (empty($info['columns']) && !$this->TableExists($table)) {
      throw new \Exception("Table {$table} does not exist.", 25663);
    }

    // If we have computed columns, it is important to know what other columns they depend on!
    $column_names = array_keys($info['columns']);
    $column_regex = implode('|', $column_names);
    foreach($info['columns'] as &$column) {
      $dependencies = array();
      if (!empty($column['definition'])) {
        $matches = array();
        if (preg_match_all("/\[[{$column_regex}\]]*\]/", $column['definition'], $matches) > 0) {
          $dependencies = array_map(function($m) { return trim($m, "[]"); }, array_shift($matches));
        }
      }
      $column['dependencies'] = array_flip($dependencies);
    }

    // Now introspect information about indexes
    $result =  $this->cnn->query_execute("select tab.[name]  as [table_name],
         idx.[name]  as [index_name],
         allc.[name] as [column_name],
         idx.[type_desc],
         idx.[is_unique],
         idx.[data_space_id],
         idx.[ignore_dup_key],
         idx.[is_primary_key],
         idx.[is_unique_constraint],
         idx.[fill_factor],
         idx.[is_padded],
         idx.[is_disabled],
         idx.[is_hypothetical],
         idx.[allow_row_locks],
         idx.[allow_page_locks],
         idxc.[is_descending_key],
         idxc.[is_included_column],
         idxc.[index_column_id],
         idxc.[key_ordinal]
    FROM sys.[tables] as tab
    INNER join sys.[indexes]       idx  ON tab.[object_id] =  idx.[object_id]
    INNER join sys.[index_columns] idxc ON idx.[object_id] = idxc.[object_id] and  idx.[index_id]  = idxc.[index_id]
    INNER join sys.[all_columns]   allc ON tab.[object_id] = allc.[object_id] and idxc.[column_id] = allc.[column_id]
    WHERE tab.object_id = OBJECT_ID(:table)
    ORDER BY tab.[name], idx.[index_id], idxc.[index_column_id]
                    ",
                  array(':table' => $schema . '.' . $table));

    foreach ($result as $index_column) {
      if (!isset($info['indexes'][$index_column->index_name])) {
        $ic = clone $index_column;
        // Only retain index specific details.
        unset($ic->column_name);
        unset($ic->index_column_id);
        unset($ic->is_descending_key);
        unset($ic->table_name);
        unset($ic->key_ordinal);
        $info['indexes'][$index_column->index_name] = (array) $ic;
        if ($index_column->is_primary_key) {
          $info['primary_key_index'] = $ic->index_name;
        }
      }

      $index = &$info['indexes'][$index_column->index_name];
      $index['columns'][$index_column->key_ordinal] = array(
           'name' => $index_column->column_name,
           'is_descending_key' => $index_column->is_descending_key,
           'key_ordinal' => $index_column->key_ordinal,
         );

      // Every columns keeps track of what indexes it is part of.
      $info['columns'][$index_column->column_name]['indexes'][] = $index_column->index_name;
      if (isset($info['columns_clean'][$index_column->column_name])) {
        $info['columns_clean'][$index_column->column_name]['indexes'][] = $index_column->index_name;
      }
    }

    $this->cnn->Cache('sqlsrv-tabledetails')->Set($table, $info);
    return $info;
  }

  /**
   * Retrieve an array of field specs from
   * an array of field names.
   *
   * @param string $table
   *   Name of the table.
   * @param array|string $fields
   *   Name of column or list of columns to retrieve information about.
   */
  public function ColumnDetailsGet($table, $fields) {
    $info = $this->TableDetailsGet($table);
    if (is_array($fields)) {
      $result = array();
      foreach ($fields as $field) {
        $result[$field] = $info['columns'][$field];
      }
      return $result;
    }
    else {
      return $info['columns'][$fields];
    }
  }

  /**
   * Summary of EngineVersion
   *
   * @return Scheme\EngineVersion
   */
  public function EngineVersion() {
    if ($cache = $this->cnn->Cache('sqlsrv-engine')->Get('EngineVersion')) {
      return $cache->data;
    }
    $version = Scheme\EngineVersion::Get($this->cnn);
    $this->cnn->Cache('sqlsrv-engine')->Set('EngineVersion', $version);
    return $version;
  }

  /**
   * Retrieve Major engine version number as integer.
   *
   * @return int
   */
  public function EngineVersionNumber() {
    $version = $this->EngineVersion();
    $parts = explode($version->Version(), '.');
    return (int) reset($parts);
  }

  /**
   * Find if a table function exists.
   *
   * @param $function
   *   Name of the function.
   *
   * @return
   *   True if the function exists, false otherwise.
   */
  public function functionExists($function) {
    // FN = Scalar Function
    // IF = Inline Table Function
    // TF = Table Function
    // FS | AF = Assembly (CLR) Scalar Function
    // FT | AT = Assembly (CLR) Table Valued Function
    return $this->cnn
      ->query_execute("SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID('" . $function . "') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT', N'AF')")
      ->fetchField() !== FALSE;
  }

  /**
   * Check if CLR is enabled. Required to run GROUP_CONCAT.
   *
   * @return bool
   */
  public function CLREnabled() {
    return $this->cnn
        ->query_execute("SELECT CONVERT(int, [value]) as [enabled] FROM sys.configurations WHERE name = 'clr enabled'")
        ->fetchField() !== 0;
  }

  /**
   * Check if a column is of variable length.
   */
  private function IsVariableLengthType($type) {
    $types = array('nvarchar' => TRUE, 'ntext' => TRUE, 'varchar' => TRUE, 'varbinary' => TRUE, 'image' => TRUE);
    return isset($types[$type]);
  }

  /**
   * Estimates the row size of a clustered index.
   *
   * @see https://msdn.microsoft.com/en-us/library/ms178085.aspx
   *
   * @return int
   *
   */
  public function calculateClusteredIndexRowSizeBytes($table, $fields, $unique = TRUE) {
    // The fields must already be in the database to retrieve their real size.
    $info = $this->TableDetailsGet($table);

    // Specify the number of fixed-length and variable-length columns
    // and calculate the space that is required for their storage.
    $num_cols = count($fields);
    $num_variable_cols = 0;
    $max_var_size = 0;
    $max_fixed_size = 0;
    foreach ($fields as $field) {
      if ($this->IsVariableLengthType($info['columns'][$field]['type'])) {
        $num_variable_cols++;
        $max_var_size += $info['columns'][$field]['max_length'];
      }
      else {
        $max_fixed_size += $info['columns'][$field]['max_length'];
      }
    }

    // If the clustered index is nonunique, account for the uniqueifier column.
    if (!$unique) {
      $num_cols++;
      $num_variable_cols++;
      $max_var_size += 4;
    }

    // Part of the row, known as the null bitmap, is reserved to manage column nullability. Calculate its size.
    $null_bitmap = 2 + (($num_cols + 7) / 8);

    // Calculate the variable-length data size.
    $variable_data_size = empty($num_variable_cols) ? 0 : 2 + ($num_variable_cols * 2) + $max_var_size;

    // Calculate total row size.
    $row_size = $max_fixed_size + $variable_data_size + $null_bitmap + 4;

    return $row_size;
  }

  /**
   * Create a database.
   *
   * @param string $name
   *   Name of the database.
   * @param string $collation
   *   Collation or empty for the default engine collation.
   */
  public function DatabaseCreate($name, $collation = NULL) {
    // Create the database.
    if ($collation !== NULL) {
      $this->cnn->query_execute("CREATE DATABASE $name COLLATE " . $collation);
    }
    else {
      $this->cnn->query_execute("CREATE DATABASE $name");
    }
  }

  /**
   * Change the database recovery model.
   *
   * @param RecoveryModel $model
   *   The model to update to.
   */
  public function setRecoveryModel(RecoveryModel $model) {
    $this->cnn->query("ALTER {$this->cnn->options['name']} model SET RECOVERY {$model->__toString()}");
  }

  /**
   * Return size information
   *
   * @param string $database
   *   Name of the database.
   * @return mixed
   */
  public function getSizeInfo($database) {

    $sql = <<< EOF
      SELECT
    DB_NAME(db.database_id) DatabaseName,
    (CAST(mfrows.RowSize AS FLOAT)*8)/1024 RowSizeMB,
    (CAST(mflog.LogSize AS FLOAT)*8)/1024 LogSizeMB,
    (CAST(mfstream.StreamSize AS FLOAT)*8)/1024 StreamSizeMB,
    (CAST(mftext.TextIndexSize AS FLOAT)*8)/1024 TextIndexSizeMB
FROM sys.databases db
    LEFT JOIN (SELECT database_id, SUM(size) RowSize FROM sys.master_files WHERE type = 0 GROUP BY database_id, type) mfrows ON mfrows.database_id = db.database_id
    LEFT JOIN (SELECT database_id, SUM(size) LogSize FROM sys.master_files WHERE type = 1 GROUP BY database_id, type) mflog ON mflog.database_id = db.database_id
    LEFT JOIN (SELECT database_id, SUM(size) StreamSize FROM sys.master_files WHERE type = 2 GROUP BY database_id, type) mfstream ON mfstream.database_id = db.database_id
    LEFT JOIN (SELECT database_id, SUM(size) TextIndexSize FROM sys.master_files WHERE type = 4 GROUP BY database_id, type) mftext ON mftext.database_id = db.database_id
    WHERE DB_NAME(db.database_id) = :database
EOF
;
    // Database is defaulted from active connection.
    $result = NULL;
    try {
      $result = $this->cnn->query_execute($sql, array(':database' => $database))->fetchObject();
    }
    catch (\Exception $e) {
      // This might not work on azure...
    }

    // There is a chance that this might not return the database size, so
    // try another strategy.
    if (empty($result->RowSizeMB)) {
      $sql = <<< EOF
      SELECT (SUM(reserved_page_count) * 8192) / 1024 / 1024 AS DbSizeInMB
      FROM    sys.dm_db_partition_stats
EOF;
      $result->RowSizeMB = $this->cnn->query_execute($sql)->fetchField();
    }

    // Try and get the number of tables
    $sql = <<< EOF
      SELECT COUNT(*) from information_schema.tables
      WHERE table_type = 'base table'
EOF;

    $result->TableCount = $this->cnn->query_execute($sql)->fetchField();

    return $result;

  }
  /**
   * Get general database information.
   *
   * @param string $database
   *   Name of the database.
   *
   * @return mixed
   */
  public function getDatabaseInfo($database) {
    static $result;
    if (isset($result)) {
      return $result;
    }
    $sql = <<< EOF
      select name
        , db.snapshot_isolation_state
        , db.snapshot_isolation_state_desc
        , db.is_read_committed_snapshot_on
        , db.recovery_model
        , db.recovery_model_desc
        , db.collation_name
    from sys.databases db
    WHERE DB_NAME(db.database_id) = :database
EOF
;
    // Database is defaulted from active connection.
    $result = $this->cnn->query_execute($sql, array(':database' => $database))->fetchObject();
    return $result;
  }
  /**
   * Get the collation of current connection wether
   * it has or not a database defined in it.
   *
   * @param string $database
   *   Name of the database.
   * @param string $schema
   *   Name of the schema.
   * @param string $table
   *   Name of the table.
   * @param string $column
   *   Name of the column.
   *
   * @return string
   */
  public function getCollation($database, $schema, $table = NULL, $column = NULL) {
    // No table or column provided, then get info about
    // database (if exists) or server defaul collation.
    if (empty($table) && empty($column)) {
      // Database is defaulted from active connection.
      if (!empty($database)) {
        // Default collation for specific table.
        $sql = "SELECT CONVERT (varchar, DATABASEPROPERTYEX('$database', 'collation'))";
        return $this->cnn->query_execute($sql)->fetchField();
      }
      else {
        // Server default collation.
        $sql = "SELECT SERVERPROPERTY ('collation') as collation";
        return $this->cnn->query_execute($sql)->fetchField();
      }
    }
    $sql = <<< EOF
      SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLLATION_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ':schema'
        AND TABLE_NAME = ':table'
        AND COLUMN_NAME = ':column'
EOF
;
    $params = array();
    $params[':schema'] = $schema;
    $params[':table'] = $table;
    $params[':column'] = $column;
    $result = $this->cnn->query_execute($sql, $params)->fetchObject();
    return $result->COLLATION_NAME;
  }
}