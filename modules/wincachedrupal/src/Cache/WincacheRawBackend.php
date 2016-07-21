<?php

/**
 * @file
 * Contains \Drupal\wincachedrupal\Cache\WincacheRawBackend.
 */

namespace Drupal\wincachedrupal\Cache;

use Drupal\supercache\Cache\CacheRawBackendInterface;

/**
 * Stores cache items in Wincache.
 */
class WincacheRawBackend extends WincacheBackendGeneric implements CacheRawBackendInterface {

  /**
   * Constructs a new WincacheRawBackend instance.
   *
   * @param string $bin
   *   The name of the cache bin.
   * @param string $site_prefix
   *   The prefix to use for all keys in the storage that belong to this site.
   */
  public function __construct($bin, $site_prefix) {
    parent::__construct('rawcache_' . $bin, $site_prefix);
  }

  /**
   * {@inheritdoc}
   */
  public function get($cid) {
    $success = FALSE;
    $data = wincache_ucache_get($this->getBinKey($cid), $success);
    if (!$success) {
      return FALSE;
    }
    return $this->prepareItem($cid, $data);
  }

  /**
   * {@inheritdoc}
   */
  public function getMultiple(&$cids) {
    // Translate the requested cache item IDs to APCu keys.
    $map = array();
    foreach ($cids as $cid) {
      $map[$this->getBinKey($cid)] = $cid;
    }

    $result = wincache_ucache_get(array_keys($map));
    $cache = array();
    if ($result) {
      foreach ($result as $key => $item) {
        $item = $this->prepareItem($key, $item);
        $cache[$map[$key]] = $item;
      }
    }
    unset($result);

    $cids = array_diff($cids, array_keys($cache));
    return $cache;
  }

  /**
   * Returns all cached items, optionally limited by a cache ID prefix.
   *
   * Wincache is a memory cache, shared across all server processes. To prevent
   * cache item clashes with other applications/installations, every cache item
   * is prefixed with a unique string for this site. Therefore, functions like
   * apc_clear_cache() cannot be used, and instead, a list of all cache items
   * belonging to this application need to be retrieved through this method
   * instead.
   *
   * @param string $prefix
   *   (optional) A cache ID prefix to limit the result to.
   *
   * @return string[]
   *   An array containing matched items.
   */
  public function getAll($prefix = '') {
    $keys = $this->getAllKeys($prefix);
    $result = $this->getMultiple($keys);
    return $result;
  }

  /**
   * Return all keys of cached items.
   *
   * @param string $prefix
   *   (optional) A cache ID prefix to limit the result to.
   * @return array
   *   An array containing matched keys.
   */
  public function getAllKeys($prefix = '') {
    $key = $this->getBinKey($prefix);
    return $this->getAllKeysWithPrefix($key);
  }

  /**
   * Prepares a cached item.
   *
   * Checks that the item is either permanent or did not expire.
   *
   * @param string $cid
   *   The cache id.
   * @param mixed $data
   *   An item retrieved from the cache.
   *
   * @return \stdClass
   *   The cache item as a Drupal cache object.
   */
  protected function prepareItem($cid, $data) {
    return (object) array('data' => $data, 'cid' => $cid);
  }

  /**
   * {@inheritdoc}
   */
  public function set($cid, $data, $expire = CacheRawBackendInterface::CACHE_PERMANENT) {
    $this->wincacheSet($this->getBinKey($cid), $data, $expire);
  }

  /**
   * {@inheritdoc}
   */
  public function setMultiple(array $items = array()) {
    foreach ($items as $cid => $item) {
      $this->set($cid, $item['data'], isset($item['expire']) ? $item['expire'] : CacheRawBackendInterface::CACHE_PERMANENT);
    }
  }

  /**
   * {@inheritdoc}
   */
  public function delete($cid) {
    wincache_ucache_delete($this->getBinKey($cid));
  }

  /**
   * {@inheritdoc}
   */
  public function deleteMultiple(array $cids) {
    wincache_ucache_delete(array_map(array($this, 'getBinKey'), $cids));
  }

  /**
   * {@inheritdoc}
   */
  public function garbageCollection() {
    parent::garbageCollectInvalidations();
  }

  /**
   * {@inheritdoc}
   */
  public function removeBin() {
    parent::invalidateBinary();
  }

  /**
   * {@inheritdoc}
   */
  public function deleteAll() {
    parent::invalidateBinary();
  }

  /**
   * {@inheritdoc}
   */
  public function counter($cid, $increment, $default = 0) {
    $success = FALSE;
    $key = $this->getBinKey($cid);
    @wincache_ucache_inc($key, $increment, $success);
    if (!$success) {
      $this->wincacheSet($key, $default);
    }
  }

  /**
   * {@inheritdoc}
   */
  public function counterMultiple(array $cids, $increment, $default = 0) {
    foreach ($cids as $cid) {
      $this->counter($cid, $increment, $default);
    }
  }

  /**
   * {@inheritdoc}
   */
  public function counterSet($cid, $value) {
    $this->set($cid, (int) $value);
  }

  /**
   * {@inheritdoc}
   */
  public function counterSetMultiple(array $items) {
    foreach ($items as $cid => $item) {
      $this->counterSet($cid, (int) $item);
    }
  }

  /**
   * {@inheritdoc}
   */
  public function counterGet($cid) {
    if ($result = $this->get($cid)) {
      return (int) $result->data;
    }
    return FALSE;
  }

  /**
   * {@inheritdoc}
   */
  public function counterGetMultiple(array &$cids) {
    $results = $this->getMultiple($cids);
    $counters = [];
    foreach ($results as $cid => $item) {
      $counters[$cid] = (int) $item->data;
    }
    return $counters;
  }

}
