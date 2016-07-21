<?php

namespace Drupal\wincachedrupal\Tests\Cache;

use Drupal\supercache\Tests\Generic\Cache\BackendGeneralTestCase;

use Drupal\wincachedrupal\Cache\WincacheRawBackendFactory;
use Drupal\supercache\Cache\DummyTagChecksum;

trait RawBackendGeneralTestCaseTrait {

  public function setUp() {
    wincache_ucache_clear();
    $app_root = '/';
    $site_path = uniqid();
    $factory = new WincacheRawBackendFactory($app_root, $site_path);

    // The aim of this setup is to get two functional backend instances.
    $this->backend = $factory->get('test_binary');
    $this->backend2 = $factory->get('test_binary_alt');

    parent::setUp();
  }

  public function tearDown() {
  }

}
