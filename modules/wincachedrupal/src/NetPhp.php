<?php

/**
 * @file
 * Contains \Drupal\wincachedrupal\NetPhp.
 */

namespace Drupal\wincachedrupal;

/**
 * Class to retain a single .Net runtime.
 */
class NetPhp {

  /**
   * The NetPhp Manager
   *
   * @var \NetPhp\Core\NetPhpRuntime
   */
  protected $runtime;

  /**
   * The NetPhp Manager
   *
   * @var \NetPhp\Core\NetProxy
   */
  protected $minifier = FALSE;

  /**
   * Load status.
   *
   * @var array
   */
  protected $status = [
      'com_enabled' => FALSE,
      'netphp' => FALSE,
    ];

  /**
   * Returns TRUE or FALSE
   *
   * @return string
   */
  public function hasComSupport() {
    return $this->status['com_enabled'];
  }

  /**
   * Returns TRUE or the error message obtained
   * while trying to the the NetPhp instance.
   *
   * @return TRUE|string
   */
  public function hasNetPhpSupport() {
    return $this->status['netphp'];
  }

  /**
   * Gets a NetPhp instance.
   *
   */
  function __construct() {

    // Check if extension loaded.
    if (!extension_loaded('com_dotnet')) {
      return;
    }

    // Check if COM and DOTNET exists.
    if (!class_exists('COM') && !class_exists('DOTNET')) {
      return;
    }

    // Up to here we have COM support.
    $this->status['com_enabled'] = TRUE;

    try {
      if (!class_exists(\NetPhp\Core\NetPhpRuntime::class)) {
        $this->status['netphp'] = 'NetPhpRuntime class not found. Make sure you run composer drupal-update.';
        return;
      }
      $this->runtime = new \NetPhp\Core\NetPhpRuntime('COM', '{2BF990C8-2680-474D-BDB4-65EEAEE0015F}');
      $this->runtime->Initialize();
      $this->status['netphp'] = TRUE;
    }
    catch (\Exception $e) {
      $this->status['netphp'] = $e->getMessage();
      return;
    }
  }

  /**
   * Summary of getRuntime
   *
   * @return \NetPhp\Core\NetPhpRuntime
   */
  public function getRuntime() {
    return $this->runtime;
  }

  #region Minifier

  protected $ajaxmin_support = FALSE;

  /**
   * Returns TRUE if there is support,
   * or the error string if not.
   *
   * @return TRUE|string
   */
  public function hasAjaxMinSupport() {
    $this->getMinifier();
    return $this->ajaxmin_support;
  }

  /**
   * Get the path where the AjaxMin library should
   * be deployed.
   *
   * @return string
   */
  public function ajaxMinPath() {
    return 'libraries/_bin/ajaxmin/AjaxMin.dll';
  }

  /**
   * Asset optimizations depeend on the minifier
   * so let's make this part of the main service.
   */
  public function getMinifier() {

    if ($this->minifier !== FALSE) {
      return $this->minifier;
    }

    $this->minifier = NULL;

    if (!($this->hasComSupport() === TRUE && $this->hasNetPhpSupport() === TRUE)) {
      $this->ajaxmin_support = 'NetPhp runtime missing.';
      return NULL;
    }

    $path = drupal_realpath($this->ajaxMinPath());

    // The file must be in libraries/_bin/ajaxmin
    if ($path == FALSE) {
      $this->ajaxmin_support = "File not found: {$this->ajaxMinPath()}";
      return NULL;
    }

    try {
      $this->runtime->RegisterAssemblyFromFile($path, 'AjaxMin');
      $this->minifier = $this->runtime->TypeFromName("Microsoft.Ajax.Utilities.Minifier")->Instantiate();
    }
    catch (\Exception $e) {
      $this->ajaxmin_support = $e->getMessage();
      return NULL;
    }

    $this->ajaxmin_support = TRUE;
    return $this->minifier;
  }

  #endregion

}
