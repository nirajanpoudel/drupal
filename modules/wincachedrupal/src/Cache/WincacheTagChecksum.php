<?php

/**
 * @file
 * Contains \Drupal\wincachedrupal\Cache\wincachedrupalTagChecksum.
 */

namespace Drupal\wincachedrupal\Cache;

use Drupal\Core\Database\Connection;
use Drupal\Core\Database\SchemaObjectExistsException;

use Drupal\Core\Cache\CacheTagsChecksumInterface;
use Drupal\Core\Cache\CacheTagsInvalidatorInterface;

/**
 * Cache tags invalidations checksum implementation that uses wincache.
 * 
 * Experimental: tags cannot be shared accross applications + this storage is volatile so
 * this should be used carefully, or not used at all.
 * 
 */
class WincacheTagChecksum implements CacheTagsChecksumInterface, CacheTagsInvalidatorInterface {

  /**
   * Contains already loaded cache invalidations from the database.
   *
   * @var array
   */
  protected $tagCache = array();

  /**
   * A list of tags that have already been invalidated in this request.
   *
   * Used to prevent the invalidation of the same cache tag multiple times.
   *
   * @var array
   */
  protected $invalidatedTags = array();

  /**
   * Constructs a WincacheTagChecksum object.
   *
   */
  public function __construct() {
  }

  /**
   * {@inheritdoc}
   */
  public function invalidateTags(array $tags) {
    foreach ($tags as $tag) {
      // Only invalidate tags once per request unless they are written again.
      if (isset($this->invalidatedTags[$tag])) {
        continue;
      }
      $success = wincache_ucache_inc('cache_tag::' . $tag, 1, $success);
      // Increment does not work if the item does not exist already.
      if ($success === FALSE) {
        wincache_ucache_set('cache_tag::' . $tag, 1);
      }
      $this->invalidatedTags[$tag] = TRUE;
      unset($this->tagCache[$tag]);
    }
  }

  /**
   * {@inheritdoc}
   */
  public function getCurrentChecksum(array $tags) {
    // Remove tags that were already invalidated during this request from the
    // static caches so that another invalidation can occur later in the same
    // request. Without that, written cache items would not be invalidated
    // correctly.
    foreach ($tags as $tag) {
      unset($this->invalidatedTags[$tag]);
    }
    return $this->calculateChecksum($tags);
  }

  /**
   * {@inheritdoc}
   */
  public function isValid($checksum, array $tags) {
    return $checksum == $this->calculateChecksum($tags);
  }

  /**
   * Calculates the current checksum for a given set of tags.
   *
   * @param array $tags
   *   The array of tags to calculate the checksum for.
   *
   * @return int
   *   The calculated checksum.
   */
  protected function calculateChecksum(array $tags) {
    $missing_tags = array_diff($tags, array_keys($this->tagCache));

    if ($missing_tags) {
      $keys = preg_filter('/^/', 'cache_tag::', $tags);
      $stored_tags = wincache_ucache_get($keys);
      $this->tagCache += $stored_tags;
      // Fill static cache with empty objects for tags not found in the database.
      $this->tagCache += array_fill_keys(array_diff($missing_tags, array_keys($stored_tags)), 0);
    }

    $real_tags = array_intersect_key($this->tagCache, array_flip($tags));
    return array_sum($real_tags);
  }

  /**
   * {@inheritdoc}
   */
  public function reset() {
    $this->tagCache = array();
    $this->invalidatedTags = array();
  }
}
