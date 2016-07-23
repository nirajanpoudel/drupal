<?php

namespace mssql\Component;

class SettingsManager {
  /**
   * Stored settings.
   *
   * @var mixed
   */
  private $settings = array();

  /**
   * Store or retrieve a setting.
   *
   * @param string $method
   * @param array $args
   * @return mixed
   */
  protected function &CallMethod($method, array $options = array(), array $args = array(), $default = NULL) {
    if (empty($args)) {
      if (!isset($this->settings[$method])) {
        $this->settings[$method] = $default;
      }

      return  $this->settings[$method];
    }

    $value = reset($args);

    if (!empty($options)) {
      if (!in_array($value, $options)) {
        throw new \Exception("Invalid value");
      }
    }

    $this->settings[$method] = $value;

    return $this->settings[$method];
  }
  /**
   * Retrieve the raw settings.
   * 
   * @return array
   */
  public function getSettings() {
    return $this->settings;
  }

}