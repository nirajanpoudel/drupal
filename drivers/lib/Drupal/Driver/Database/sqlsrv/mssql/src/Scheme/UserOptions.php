<?php

namespace mssql\Scheme;

use mssql\Component\SettingsManager;

use mssql\Connection;

class UserOptions extends SettingsManager {

  /**
   * Get an instance of UserOptions
   *
   * @param Connection $connection
   *
   * @return UserOptions
   */
  public static function Get(Connection $connection) {

    $data = new UserOptions();

    try {

      $result = $connection->query_execute('DBCC UserOptions')->fetchAllKeyed();

      // These are not available on AZURE ?
      $data->QuotedIdentifier($result['quoted_identifier']);
      $data->AnsiNullDefaultOn($result['ansi_null_dflt_on']);
      $data->AnsiWarnings($result['ansi_warnings']);
      $data->AnsiPadding($result['ansi_padding']);
      $data->AnsiNulls($result['ansi_nulls']);
      $data->ConcatNullYieldsNull( $result['concat_null_yields_null']);

    }
    catch (\Exception $e) {

      // Azure compatibility.
      $result = [];
      $result['textsize'] =  $connection->query_execute("SELECT @@TEXTSIZE AS [textsize]")->fetchColumn();
      $result['language'] =  $connection->query_execute("SELECT @@LANGUAGE AS [language]")->fetchColumn();
      $result['dateformat'] =  $connection->query_execute("SELECT [dateformat] FROM [sys].[syslanguages] WHERE [langid] = @@LANGID")->fetchColumn();
      $result['datefirst'] =  $connection->query_execute("select @@DATEFIRST as [datefirst]")->fetchColumn();
      $result['lock_timeout'] =  $connection->query_execute("select @@lock_timeout as [lock_timeout]")->fetchColumn();

      $query = <<<EOT
        SELECT CASE transaction_isolation_level 
        WHEN 0 THEN 'Unspecified' 
        WHEN 1 THEN 'Read Uncomitted' 
        WHEN 2 THEN 'Read comitted' 
        WHEN 3 THEN 'Repeatable' 
        WHEN 4 THEN 'Serializable' 
        WHEN 5 THEN 'Snapshot' END AS TRANSACTION_ISOLATION_LEVEL 
        FROM sys.dm_exec_sessions 
        where session_id = @@SPID
EOT;

      $result['isolation level'] =  $connection->query_execute($query)->fetchColumn();

    }

    // These fields are common to both MSSQL and Azure
    $data->TextSize($result['textsize']);
    $data->Language($result['language']);
    $data->DateFormat($result['dateformat']);
    $data->DateFirst($result['datefirst']);
    $data->LockTimeout($result['lock_timeout']);
    $data->IsolationLevel($result['isolation level']);

    return $data;
  }

  /**
   * @return string
   */
  public function TextSize() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args());
  }

  /**
   * @return string
   */
  public function Language() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args());
  }

  /**
   * @return string
   */
  public function DateFormat() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args());
  }

  /**
   * @return string
   */
  public function DateFirst() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args(), NULL);
  }

  public function LockTimeout() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args(), NULL);
  }

  public function QuotedIdentifier() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args(), NULL);
  }

  public function Arithabort() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args(), NULL);
  }

  public function AnsiNullDefaultOn() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args(), NULL);
  }

  public function AnsiWarnings() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args(), NULL);
  }

  public function AnsiPadding() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args(), NULL);
  }

  public function AnsiNulls() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args(), NULL);
  }

  public function ConcatNullYieldsNull() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args(), NULL);
  }

  public function IsolationLevel() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args(), NULL);
  }

}