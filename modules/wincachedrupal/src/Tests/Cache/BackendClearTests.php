<?php

namespace Drupal\wincachedrupal\Tests\Cache;

use Drupal\supercache\Tests\Generic\Cache\BackendGeneralTestCase;
use Drupal\supercache\Tests\Generic\Cache\BackendClearTests as ClearTests;

use Drupal\Core\Site\Settings;

class BackendClearTests extends ClearTests {
  use BackendGeneralTestCaseTrait;
}
