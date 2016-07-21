<?php

/**
 * @file
 * Contains \Drupal\wincachedrupal\Cache\wincachedrupalBackendFactory.
 */

namespace Drupal\wincachedrupal\Cache;

use Drupal\Core\Site\Settings;

use Drupal\Core\Cache\CacheFactoryInterface;
use Drupal\Core\Cache\CacheTagsChecksumInterface;

class WincacheBackendFactory implements CacheFactoryInterface {

  /**
   * The site prefix string.
   *
   * @var string
   */
  protected $sitePrefix;

  /**
   * The cache tags checksum provider.
   *
   * @var \Drupal\Core\Cache\CacheTagsChecksumInterface
   */
  protected $checksumProvider;

  /**
   * Constructs an WincacheBackendFactory object.
   *
   * @param string $root
   *   The app root.
   * @param string $site_path
   *   The site path.
   * @param \Drupal\Core\Cache\CacheTagsChecksumInterface $checksum_provider
   *   The cache tags checksum provider.
   */
  public function __construct($root, $site_path, CacheTagsChecksumInterface $checksum_provider) {
    $this->sitePrefix = Settings::getApcuPrefix('wincache_backend', $root, $site_path);
    $this->checksumProvider = $checksum_provider;
  }

  /**
   * Gets WincacheBackend for the specified cache bin.
   *
   * @param $bin
   *   The cache bin for which the object is created.
   *
   * @return WincacheBackend
   *   The cache backend object for the specified cache bin.
   */
  public function get($bin) {
    return new WincacheBackend($bin, $this->sitePrefix, $this->checksumProvider);
  }
}
