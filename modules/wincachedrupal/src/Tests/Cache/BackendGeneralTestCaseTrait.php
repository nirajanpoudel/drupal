<?php

namespace Drupal\wincachedrupal\Tests\Cache;

use Drupal\supercache\Tests\Generic\Cache\BackendGeneralTestCase;

use Drupal\wincachedrupal\Cache\WincacheBackendFactory;
use Drupal\supercache\Cache\DummyTagChecksum;

trait BackendGeneralTestCaseTrait {

  public function setUp() {
    wincache_ucache_clear();
    $app_root = '/';
    $site_path = uniqid();
    $factory = new WincacheBackendFactory($app_root, $site_path, new DummyTagChecksum());

    // The aim of this setup is to get two functional backend instances.
    $this->backend = $factory->get('test_binary');
    $this->backend2 = $factory->get('test_binary_alt');

    parent::setup();
  }

  public function tearDown() {

  }
}
