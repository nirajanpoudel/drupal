<?php

namespace mssql\Component;

interface CacheInterface {

  /**
   * Set a cache item.
   *
   * @param string $cid
   * @param mixed $data
   */
  function Set($cid, $data);

  /**
   * Get a cache item.
   *
   * @param mixed $cid
   */
  function Get($cid);

  /**
   * Clear a cache item.
   *
   * @param string $cid
   */
  function Clear($cid);

}