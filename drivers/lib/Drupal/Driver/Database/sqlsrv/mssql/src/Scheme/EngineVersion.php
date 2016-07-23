<?php

namespace mssql\Scheme;

use mssql\Component\SettingsManager;

use mssql\Connection;

class EngineVersion extends SettingsManager {

  /**
   * Get an instance of EngineVersion
   *
   * @param Connection $cnn
   *   The connection to use
   *
   * @return EngineVersion
   */
  public static function Get(Connection $cnn) {
    $data = $cnn
    ->query_execute(<<< EOF
    SELECT CONVERT (varchar,SERVERPROPERTY('productversion')) AS VERSION,
    CONVERT (varchar,SERVERPROPERTY('productlevel')) AS LEVEL,
    CONVERT (varchar,SERVERPROPERTY('edition')) AS EDITION
EOF
    )->fetchAssoc();

    $result = new EngineVersion();
    $result->Version($data['VERSION']);
    $result->Level($data['LEVEL']);
    $result->Edition($data['EDITION']);

    return $result;
  }

  public function Version() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args());
  }

  public function Level() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args());
  }

  public function Edition() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args());
  }
}