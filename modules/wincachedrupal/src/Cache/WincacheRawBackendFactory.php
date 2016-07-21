<?php

/**
 * @file
 * Contains \Drupal\wincachedrupal\Cache\WincacheRawBackendFactory.
 */

namespace Drupal\wincachedrupal\Cache;

use Drupal\Core\Site\Settings;
use Drupal\supercache\Cache\CacheRawFactoryInterface;

class WincacheRawBackendFactory implements CacheRawFactoryInterface {

  /**
   * The cache tags checksum provider.
   *
   * @var string
   */
  protected $sitePrefix;

  /**
   * Constructs the WincacheRawBackendFactory
   * 
   * @param string $root
   *   The site's root.
   * @param string $site_path 
   *   The site's path.
   */
  function __construct($root, $site_path) {
    $this->sitePrefix = Settings::getApcuPrefix('wincache_backend', $root, $site_path);
  }

  /**
   * Gets DatabaseBackend for the specified cache bin.
   *
   * @param $bin
   *   The cache bin for which the object is created.
   *
   * @return WincacheRawBackend
   *   The cache backend object for the specified cache bin.
   */
  function get($bin) {
    return new WincacheRawBackend($bin, $this->sitePrefix);
  }

}
