<?php

namespace mssql\Component;

class CacheFactoryDefault implements CacheFactoryInterface {
  /**
   * Unique prefix for this site/database
   *
   * @param string $prefix
   *   Unique prefix for this site/database
   */
  public function __construct($prefix) {
    $this->prefix = $prefix;
  }
  /**
   * Unique prefix for this database
   * 
   * @var string
   */
  protected $prefix;
  /**
   * List of already loaded cache binaries.
   *
   * @var CacheInterface[]
   */
  protected $binaries = array();
  /**
   * {@inhertidoc}
   */
  public function get($bin) {
    $name = $this->prefix . ':' . $bin;
    if (!isset($this->binaries[$name])) {
      $this->binaries[$name] = new CacheWincache($name);
    }
    return $this->binaries[$name];
  }
}