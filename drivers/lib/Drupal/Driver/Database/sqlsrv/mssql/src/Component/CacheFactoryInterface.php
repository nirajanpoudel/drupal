<?php

namespace mssql\Component;

interface CacheFactoryInterface {

  /**
   * Get a cache backend for a specific binary.
   *
   * @param  string $bin
   *
   * @return CacheInterface
   */
  function get($bin);
}