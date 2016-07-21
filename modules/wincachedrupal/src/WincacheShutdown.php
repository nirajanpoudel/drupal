<?php

namespace Drupal\wincachedrupal;

use \Psr\Log\LoggerInterface;

class WincacheShutdown {

  /**
   * Logger channel.
   *
   * @var LoggerInterface
   */
  var $logger;

  /**
   * Some shutdown verifications.
   */
  public function shutdown() {

    $this->logger = \Drupal::logger('wincachedrupal');

    static::wincachedrupal_check_scache();
    static::wincachedrupal_check_ucache();
  }

  /**
   * Check the status of the user cache, and if full,
   * clear data.
   */
  private function wincachedrupal_check_ucache() {
    $threshold = 0.1;
    // Make sure that the user cache is not FULL
    $user_cache_available = function_exists('wincache_ucache_info') && !strcmp(ini_get('wincache.ucenabled'), "1");
    if (!$user_cache_available) {
      return;
    }
    $ucache_mem_info = wincache_ucache_meminfo();
    // Under some situations WincacheDrupal will fail to report
    // any data through wincache_ucache_meminfo().
    if (!empty($ucache_mem_info) && $ucache_mem_info['memory_total'] > 0) {
      $ucache_available_memory = $ucache_mem_info['memory_total'] - $ucache_mem_info['memory_overhead'];
      $free_memory_ratio = $ucache_mem_info['memory_free'] / $ucache_available_memory;
      // If free memory is below 10% of total
      // do a cache wipe!
      if ($free_memory_ratio < $threshold) {
        $params = array();
        $params["@free"] = round($ucache_mem_info['memory_free'] / 1024, 0);
        $params["@total"] = round($ucache_mem_info['memory_total'] / 1024, 0);
        $params["@avail"] = round($ucache_available_memory / 1024, 0);
        $this->logger->notice('Usercache threshold limit reached. @free Kb free out of @avail Kb available from a total of @total Kb. Cache cleared.', $params);
        wincache_ucache_clear();
      }
    }

  }

  /**
   * Check the status of the session cache and if full,
   * clear sessions.
   */
  private function wincachedrupal_check_scache() {
    $threshold = 0.1;
    if (!function_exists('wincache_scache_meminfo')) {
      return;
    }
    // Make sure that the session cache is not FULL! Otherwise people will not be able to login anymore...
    $scache_mem_info = wincache_scache_meminfo();
    if (!empty($scache_mem_info) && $scache_mem_info['memory_total'] > 0) {
      $scache_available_memory = $scache_mem_info['memory_total'] - $scache_mem_info['memory_overhead'];
      $free_memory_ratio = $scache_mem_info['memory_free'] / $scache_available_memory;
      if ($free_memory_ratio < $threshold) {
        // There is no way of clearing sessions...
        // but this one!
        $current_id = session_id();
        $scache_info = wincache_scache_info();
        // Destroy all session.
        foreach ($scache_info['scache_entries'] as $entry) {
          // Do not delete own session.
          if ($current_id == $entry['key_name']) {
            continue;
          }
          session_id($entry['key_name']);
          session_start();
          session_destroy();
          session_write_close();
        }
        // When there is only one session and that is ours,
        // calling session_start will fail because current
        // session was not destroyed.
        if (session_status() == PHP_SESSION_NONE) {
          session_id($current_id);
          session_start();
        }
        $params = array();
        $params["@free"] = round($scache_mem_info['memory_free'] / 1024, 0);
        $params["@total"] = round($scache_mem_info['memory_total'] / 1024, 0);
        $params["@avail"] = round($scache_available_memory / 1024, 0);
        $this->logger->notice('SessionCache threshold limit reached. @free Kb free out of @avail Kb available from a total of @total Kb. Items cleared.', $params);
      }
    }
  }
}
