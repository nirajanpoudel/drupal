<?php

/**
 * @file
 * Contains \Drupal\wincachedrupal\Asset\JsOptimizer.
 */

namespace Drupal\wincachedrupal\Asset;

use Drupal\Component\Utility\Unicode;
use Drupal\Core\Asset\JsOptimizer as CoreOptimizer;

/**
 * Optimizes a JavaScript asset.
 */
class JsOptimizer extends CoreOptimizer {

  /**
   * Code settings instance for reuse.
   *
   * @var \NetPhp\Core\NetProxy
   */
  protected $codeSettings;

  /**
   * Minifier instance, for reuse.
   *
   * @var \NetPhp\Core\NetProxy
   */
  protected $minifier;

  /**
   * Returns an instance of JsOptimizer
   *
   * @param JsOptimizer $netphp
   */
  public function __construct(\Drupal\wincachedrupal\NetPhp $netphp) {
    if ($this->minifier = $netphp->getMinifier()) {
      $runtime = $netphp->getRuntime();
      $this->codeSettings = $runtime->TypeFromName("Microsoft.Ajax.Utilities.CodeSettings")->Instantiate();
      $this->codeSettings->OutputMode = $runtime->TypeFromName("Microsoft.Ajax.Utilities.OutputMode")->Instantiate()->Enum('SingleLine');
      $this->codeSettings->QuoteObjectLiteralProperties = TRUE;
    }
  }

  /**
   * {@inheritdoc}
   */
  public function optimize(array $js_asset) {
    // The core implementation actually does no optimization at all...
    $data = parent::optimize($js_asset);
    if ($this->minifier) {
      $data = $this->minifier->MinifyJavaScript($data, $this->codeSettings)->Val();
      $data = utf8_encode($data);
    }
    return $data;
  }
}
