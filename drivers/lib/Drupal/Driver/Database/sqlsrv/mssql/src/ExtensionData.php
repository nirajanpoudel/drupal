<?php

namespace mssql;

use mssql\Component\SettingsManager;
use mssql\Component\EmtpySetting;

class ExtensionData extends SettingsManager {

  /**
   * The name of the Extension.
   */
  public function Name() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args());
  }

  public function Version() {
    return parent::CallMethod(__FUNCTION__, array(),func_get_args());
  }

  public function ClassName() {
    return parent::CallMethod(__FUNCTION__, array(),func_get_args());
  }

  public function Constants() {
    return parent::CallMethod(__FUNCTION__, array(),func_get_args());
  }

  public function Dependencies() {
    return parent::CallMethod(__FUNCTION__, array(),func_get_args());
  }

  public function Functions() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args());
  }

  public function IniEntries() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args());
  }

  public function Persistent() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args());
  }

  public function Temporary() {
    return parent::CallMethod(__FUNCTION__, array(), func_get_args());
  }
}