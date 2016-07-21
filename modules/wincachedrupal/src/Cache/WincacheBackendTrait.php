<?php

namespace Drupal\wincachedrupal\Cache;

use Drupal\Core\Cache\CacheBackendInterface;

trait WincacheBackendTrait {

  /**
   * Current time used to validate
   * cache item expiration times.
   *
   * @var int
   */
  protected $requestTime;

  /**
   * Prefix for all keys in this cache bin.
   *
   * Includes the site-specific prefix in $sitePrefix.
   *
   * @var string
   */
  protected $binPrefix;

  /**
   * Returns a 12 character length MD5.
   *
   * @param string $string
   * @return string
   */
  public function shortMd5($string) {
    return substr(base_convert(md5($string), 16,32), 0, 12);
  }

  /**
   * Wrapper for wincache_ucache_set to properly manage expirations.
   *
   * @param string $cid
   *   The cache id.
   * @param mixed $data
   *   Data so store in the cache.
   * @param int $expire
   *   Expiration as unix timestamp.
   */
  protected function wincacheSet($cid, $data, $expire = CacheBackendInterface::CACHE_PERMANENT) {
    if ($ttl = $this->getTtl($expire)) {
      return wincache_ucache_set($cid, $data, $ttl);
    }
    else {
      return wincache_ucache_set($cid, $data);
    }
  }

  /**
   * Wrapper for wincache_ucache_add to properly manage expirations.
   *
   * @param string $cid
   *   The cache id.
   * @param mixed $data
   *   Data so store in the cache.
   * @param int $expire
   *   Expiration as unix timestamp.
   */
  protected function wincacheAdd($cid, $data, $expire) {
    $result = FALSE;
    set_error_handler(function() { /* Prevent Drupal from logging any exceptions or warning thrown here */ }, E_ALL);
    if ($ttl = $this->getTtl($expire)) {
      $result = @wincache_ucache_add($cid, $data, $ttl);
    }
    else {
      $result = @wincache_ucache_add($cid, $data);
    }
    restore_error_handler();
    return $result;
  }

  /**
   * The Cache API uses a unixtimestamp to set
   * expiration. But Wincache expects a TTL.
   *
   * @param int $expire
   *   The unix timestamp expiration or -1 for no expire.
   */
  protected function getTtl($expire) {
    if ($expire == CacheBackendInterface::CACHE_PERMANENT) {
      // If no ttl is supplied (or if the ttl is 0), the value will persist until 
      // it is removed from the cache manually, or otherwise fails to exist in the cache (clear, restart, etc.).
      return FALSE;
    }
    $result = $expire - time();
    // Weird case, this is more or less like inmediate expiration...
    if ($result <= 0) {
      return 1;
    }
    return $result;
  }

  /**
   * Retrieve all keys in wincache
   * that start with a given prefix.
   *
   * @param string $prefix
   *   The prefix keys should start with.
   * @return array
   */
  public function getAllKeysWithPrefix($prefix) {
    $data = wincache_ucache_info();
    $k = array_column($data['ucache_entries'], 'key_name');
    $keys = preg_grep("/^$prefix/", $k);
    $keys = preg_replace("/^$prefix/", '', $keys);
    return $keys;
  }

  /**
   * Refreshes the current request time.
   *
   * Uses the global REQUEST_TIME on the first
   * call and refreshes to current time on subsequent
   * calls.
   */
  public function refreshRequestTime() {
    if (empty($this->requestTime)) {
      if (defined('REQUEST_TIME')) {
        $this->requestTime = REQUEST_TIME;
        return;
      }
      if (isset($_SERVER['REQUEST_TIME_FLOAT'])) {
        $this->requestTime = round($_SERVER['REQUEST_TIME_FLOAT'], 3);
        return;
      }
    }
    $this->requestTime = round(microtime(TRUE), 3);
  }

  /**
   * Prepends the Wincache user variable prefix for this bin to a cache item ID.
   *
   * @param string $cid
   *   The cache item ID to prefix.
   *
   * @return string
   *   The Wincache key for the cache item ID.
   */
  protected function getBinKey($cid) {
    return $this->binPrefix . $cid;
  }
}
