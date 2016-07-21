<?php

/**
 * @file
 * Contains \Drupal\wincachedrupal\Cache\Cache\wincachedrupalBackend.
 */

namespace Drupal\wincachedrupal\Cache;

use Drupal\Core\Cache\CacheBackendInterface;
use Drupal\Core\Cache\CacheTagsChecksumInterface;
use Drupal\Core\Cache\Cache;

/**
 * Stores cache items in the Alternative PHP Cache User Cache (APCu).
 */
class WincacheBackend extends WincacheBackendGeneric implements CacheBackendInterface {

  /**
   * The cache tags checksum provider.
   *
   * @var \Drupal\Core\Cache\CacheTagsChecksumInterface
   */
  protected $checksumProvider;

  /**
   * Constructs a new WincacheBackend instance.
   *
   * @param string $bin
   *   The name of the cache bin.
   * @param string $site_prefix
   *   The prefix to use for all keys in the storage that belong to this site.
   * @param \Drupal\Core\Cache\CacheTagsChecksumInterface $checksum_provider
   *   The cache tags checksum provider.
   */
  public function __construct($bin, $site_prefix, CacheTagsChecksumInterface $checksum_provider) {
    $this->checksumProvider = $checksum_provider;
    parent::__construct('cache_' . $bin, $site_prefix);
  }

  /**
   * {@inheritdoc}
   */
  public function get($cid, $allow_invalid = FALSE) {
    $success = FALSE;
    $cache = wincache_ucache_get($this->getBinKey($cid), $success);
    if ($success == FALSE) {
      return FALSE;
    }
    return $this->prepareItem($cache, $allow_invalid);
  }

  /**
   * {@inheritdoc}
   */
  public function getMultiple(&$cids, $allow_invalid = FALSE) {
    // Translate the requested cache item IDs to Wincache keys.
    $map = array();
    foreach ($cids as $cid) {
      $map[$this->getBinKey($cid)] = $cid;
    }

    $success = FALSE;
    $result = wincache_ucache_get(array_keys($map), $success);
    $cache = array();
    if ($success == TRUE) {
      foreach ($result as $key => $item) {
        $item = $this->prepareItem($item, $allow_invalid);
        if ($item) {
          $cache[$map[$key]] = $item;
        }
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
   * wincache_ucache_clear() cannot be used, and instead, a list of all cache items
   * belonging to this application need to be retrieved through this method
   * instead.
   *
   * @param string $prefix
   *   (optional) A cache ID prefix to limit the result to.
   *
   * @return string[]
   *   An array of Keys for this binary.
   */
  public function getAll($prefix = '') {
    $cids = $this->getAllKeys($prefix);
    $result = $this->getMultiple($cids);
    return $result;
  }

  /**
   * Return all keys of cached items.
   * 
   * @param string $prefix 
   * @return array
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
   * @param \stdClass $cache
   *   An item loaded from cache_get() or cache_get_multiple().
   * @param bool $allow_invalid
   *   If TRUE, a cache item may be returned even if it is expired or has been
   *   invalidated. See ::get().
   *
   * @return mixed
   *   The cache item or FALSE if the item expired.
   */
  protected function prepareItem($cache, $allow_invalid) {
    if (!isset($cache->data)) {
      return FALSE;
    }

    $cache->tags = $cache->tags ? explode(' ', $cache->tags) : array();

    // Check expire time.
    $cache->valid = $cache->expire == Cache::PERMANENT || $cache->expire >= $this->requestTime;

    // Check if invalidateTags() has been called with any of the entry's tags.
    if (!$this->checksumProvider->isValid($cache->checksum, $cache->tags)) {
      $cache->valid = FALSE;
    }

    if (!$allow_invalid && !$cache->valid) {
      return FALSE;
    }

    return $cache;
  }

  protected function prepareCacheItem($cid, $data, $expire, array $tags = array()) {
    Cache::validateTags($tags);
    $tags = array_unique($tags);
    $cache = new \stdClass();
    $cache->cid = $cid;
    $cache->created = round(microtime(TRUE), 3);
    $cache->expire = $expire;
    $cache->tags = implode(' ', $tags);
    $cache->checksum = $this->checksumProvider->getCurrentChecksum($tags);
    // There is no need to serialize unserialize with wincache.
    $cache->serialized = 0;
    $cache->data = $data;
    return $cache;
  }

  /**
   * {@inheritdoc}
   */
  public function set($cid, $data, $expire = CacheBackendInterface::CACHE_PERMANENT, array $tags = array()) {
    $cache = $this->prepareCacheItem($cid, $data, $expire, $tags);
    $this->wincacheSet($this->getBinKey($cid), $cache, $expire);
  }


  /**
   * Like set() but will fail if the item already exists in the cache.
   * 
   * @param mixed $cid 
   * @param mixed $data 
   * @param mixed $expire 
   * @param array $tags 
   */
  public function add($cid, $data, $expire = CacheBackendInterface::CACHE_PERMANENT, array $tags = array()) {
    $cache = $this->prepareCacheItem($cid, $data, $expire, $tags);
    return $this->wincacheAdd($cid, $cache, $expire);
  }

  /**
   * {@inheritdoc}
   */
  public function setMultiple(array $items = array()) {
    foreach ($items as $cid => $item) {
      $this->set($cid, $item['data'], isset($item['expire']) ? $item['expire'] : CacheBackendInterface::CACHE_PERMANENT, isset($item['tags']) ? $item['tags'] : array());
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
  public function deleteAll() {
    parent::invalidateBinary();
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
  public function invalidate($cid) {
    $this->invalidateMultiple([$cid]);
  }

  /**
   * {@inheritdoc}
   */
  public function invalidateMultiple(array $cids) {
    $this->deleteMultiple($cids);
  }

  /**
   * {@inheritdoc}
   */
  public function invalidateAll() {
    $this->deleteAll();
  }

  /**
   * {@inheritdoc}
   */
  public function garbageCollection() {
    parent::garbageCollectInvalidations();
  }
}
