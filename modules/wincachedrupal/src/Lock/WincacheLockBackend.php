<?php

/**
 * @file
 * Contains \Drupal\wincachedrupal\Lock\wincachedrupalLockBackend.
 */

namespace Drupal\wincachedrupal\Lock;

use Drupal\Core\Database\Connection;
use Drupal\Core\Database\IntegrityConstraintViolationException;

use Drupal\Core\Lock\LockBackendAbstract;

use Drupal\supercache\Cache\DummyTagChecksum;
use Drupal\wincachedrupal\Cache\WincacheBackend;

/**
 * Defines the database lock backend. This is the default backend in Drupal.
 *
 * @ingroup lock
 */
class WincacheLockBackend extends LockBackendAbstract {

  /**
   * Cache storage.
   * 
   * @var WincacheBackend
   */
  protected $cache;

  /**
   * Constructs a new WincacheLockBackend.
   *
   */
  public function __construct() {
    // __destruct() is causing problems with garbage collections, register a
    // shutdown function instead.
    drupal_register_shutdown_function(array($this, 'releaseAll'));
    $this->cache = new WincacheBackend('semaphore', '', new DummyTagChecksum());
    // Initialize the lock id.
    $this->getLockId();
  }

  /**
   * Implements Drupal\Core\Lock\LockBackedInterface::acquire().
   */
  public function acquire($name, $timeout = 30.0) {

    // Insure that the timeout is at least 1 ms.
    $timeout = max($timeout, 0.001);
    $expire = microtime(TRUE) + $timeout;

    if ($this->cache->add($name, $this->lockId, $timeout + time())) {
      $this->locks[$name] = $this->lockId;
    }
    elseif (($result = $this->cache->get($name)) && isset($locks[$name]) && $this->locks[$name] == $result->data) {
      // Only renew the lock if we already set it and it has not expired.
      $this->cache->set($name, $this->lockId, $expire);
    }
    else {
      // Failed to acquire the lock.  Unset the key from the $locks array even if
      // not set, PHP 5+ allows this without error or warning.
      unset($this->locks[$name]);
    }

    return isset($locks[$name]);
  }

  /**
   * Implements Drupal\Core\Lock\LockBackedInterface::lockMayBeAvailable().
   */
  public function lockMayBeAvailable($name) {
    return !$this->cache->get($name);
  }

  /**
   * Implements Drupal\Core\Lock\LockBackedInterface::release().
   */
  public function release($name) {
    $result = $this->cache->get($name);
    if (isset($locks[$name]) && $result !== FALSE && $result->data == $this->locks[$name]) {
      $this->cache->clear($name);
    }
    // We unset unconditionally since caller assumes lock is released anyway.
    unset($this->locks[$name]);
  }

  /**
   * Implements Drupal\Core\Lock\LockBackedInterface::releaseAll().
   */
  public function releaseAll($lockId = NULL) {
    // Only attempt to release locks if any were acquired.
    if (!empty($this->locks)) {
      if (empty($lockId)) {
        $lockId = $this->getLockId();
      }
      $clears = array();
      foreach ($this->locks as $name => $id) {
        if ($cache = $this->cache->get($name)) {
          $value = $cache->data;
          if ($value == $id) {
            $clears[] = $name;
          }
        }
      }
      $this->locks = array();
      $this->cache->deleteMultiple($clears);
    }
  }
}
