<?php

namespace Drupal\wincachedrupal\Cache;

use Drupal\Core\Cache\CacheBackendInterface;

use Drupal\supercache\Cache\RequestTimeTrait;

abstract class WincacheBackendGeneric {

  use RequestTimeTrait;

  const INVALIDATIONCOUNT = '@@wincache_invalidation_count';
  const INVALIDATIONS = '@@wincache_invalidations';

  /**
   * The original name of the binary.
   *
   * @var string
   */
  protected $realBin;

  /**
   * The name of the cache bin to use.
   *
   * @var string
   */
  protected $bin;


  /**
   * Prefix for all keys in the storage that belong to this site.
   *
   * @var string
   */
  protected $sitePrefix;

  /**
   * Prefix for all keys in this cache bin.
   *
   * Includes the site-specific prefix in $sitePrefix.
   *
   * @var string
   */
  protected $binPrefix;

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
   * that start with a given prefix
   * or with a set of prefixes.
   *
   * @param string|string[] $prefixes
   *   The prefix keys should start with.
   * @return array
   */
  public function getAllKeysWithPrefix($prefixes, $remove_prefixes = TRUE) {
    if (!is_array($prefixes)) {
      $prefixes = [$prefixes];
    }
    $parts = [];
    foreach ($prefixes as $prefix) {
      $parts[] = "^$prefix";
    }
    $data = wincache_ucache_info();
    $k = array_column($data['ucache_entries'], 'key_name');
    $regex = '/' . implode('|', $parts) . '/';
    $keys = preg_grep($regex, $k);
    // We might want the cache id's without
    // the prefix.
    if ($remove_prefixes) {
      $keys = preg_replace($regex, '', $keys);
    }
    return $keys;
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
  public function getBinKey($cid) {
    return $this->binPrefix . $cid;
  }

  /**
   * Constructor.
   *
   * @param mixed $bin
   * @param mixed $site_prefix
   */
  public function __construct($bin, $site_prefix) {
    // Cache clears are slow, so what we do is keep
    // track of an invalidation count.
    $this->sitePrefix = $this->shortMd5($site_prefix);
    $this->realBin = $bin;
    $this->bin = $this->getBinName();
    $this->generateBinPrefix();
    $this->refreshRequestTime();
  }

  /**
   * Generates and updates the binPrefix from the
   * site prefix and the binary name.
   */
  protected function generateBinPrefix() {
    $this->binPrefix = $this->sitePrefix . ':' . $this->bin . ':';
  }

  /**
   * Get the real name of the binary to be used
   * considering previous invalidations.
   *
   * @param string $bin
   * @param string $site_prefix
   *
   * @return string
   */
  protected function getBinName() {
    $key = $this->sitePrefix . ":" . self::INVALIDATIONCOUNT . ':' . $this->realBin;
    $success = FALSE;
    $cache = wincache_ucache_get($key, $success);
    if ($success == FALSE) {
      wincache_ucache_set($key, 0);
      $cache = 0;
    }
    return $this->realBin . $cache;
  }

  /**
   * Inmediate invalidation of all the contents
   * of this binary.
   */
  protected function invalidateBinary() {
    $invalidationcountkey = $this->sitePrefix . ":" . self::INVALIDATIONCOUNT . ':' . $this->realBin;
    $invalidationskey = $this->sitePrefix . ":" . self::INVALIDATIONS;
    // This call MUST succeed because the value
    // is populated when calliing getBinName the
    // first time.
    $inv = wincache_ucache_inc($invalidationcountkey, 1);
    $invalidations = wincache_ucache_get($invalidationskey);
    if (empty($invalidations)) {
      $invalidations = [];
      if ($inv > 1) {
        // Back to zero.
        $inv = 0;
      }
    }
    // Store the current name so that it can
    // be clared on garbage colleciton.
    $invalidations[] = $this->binPrefix;
    wincache_ucache_set($invalidationskey, $invalidations);
    // Update the bin name.
    $this->bin = $this->realBin . $inv;
    $this->generateBinPrefix();
  }

  /**
   * Garbage collect the invalidations.
   */
  protected function garbageCollectInvalidations() {
    // The invalidation key is "global" and knows not
    // about binaries, that is to garbage collect everything
    // at once because the heavy part is the call to wincache_ucache_get
    // which can be very memory intensive.
    $invalidationskey = $this->sitePrefix . ":" . self::INVALIDATIONS;
    $invalidations = wincache_ucache_get($invalidationskey, $success);
    if (!empty($invalidations)) {
      // Invalidations contains an array of prefixes
      // used for this binary.
      wincache_ucache_delete($this->getAllKeysWithPrefix($invalidations, FALSE));
      wincache_ucache_set($invalidationskey, []);
    }
  }
}
