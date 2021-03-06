<?php

namespace mssql\Component;

use UnexpectedValueException;
use ReflectionClass;
use BadMethodCallException;

/**
 * Enum class.
 *
 */
abstract class Enum {

  /**
   * Enum value
   *
   * @var mixed
   */
  protected $value;

  /**
   * Store existing constants in a static cache per object.
   *
   * @var array
   */
  private static $cache = array();

  /**
   * Creates a new value of some type
   *
   * @param mixed $value
   *
   * @throws UnexpectedValueException
   */
  public function __construct($value) {
    if (!$this->isValid($value)) {
      throw new UnexpectedValueException("Value '$value' is not part of the enum " . get_called_class());
    }
    $this->value = $value;
  }

  /**
   * @return mixed
   */
  public function getValue() {
    return $this->value;
  }

  /**
   * Returns the enum key (i.e. the constant name).
   *
   * @return mixed
   */
  public function getKey() {
    return self::search($this->value);
  }

  /**
   * @return string
   */
  public function __toString() {
    return (string) $this->value;
  }

  /**
   * Returns the names (keys) of all constants in the Enum class
   *
   * @return array
   */
  public static function keys() {
    return array_keys(self::toArray());
  }

  /**
   * Returns instances of the Enum class of all Enum constants
   *
   * @return array Constant name in key, Enum instance in value
   */
  public static function values() {
    $values = array();
    foreach (self::toArray() as $key => $value) {
      $values[$key] = new static($value);
    }
    return $values;
  }

  /**
   * Returns all possible values as an array
   *
   * @return array Constant name in key, constant value in value
   */
  public static function toArray() {
    $class = get_called_class();
    if (!array_key_exists($class, self::$cache)) {
      $reflection = new ReflectionClass($class);
      self::$cache[$class] = $reflection->getConstants();
    }
    return self::$cache[$class];
  }
  /**
   *
   * Check if is valid enum value
   *
   * @param $value
   * @return bool
   */
  public static function isValid($value) {
    return in_array($value, self::toArray(), true);
  }

  /**
   * Check if is valid enum key
   *
   * @param $key
   *
   * @return bool
   */
  public static function isValidKey($key) {
    $array = self::toArray();
    return isset($array[$key]);
  }

  /**
   * Return key for value
   *
   * @param $value
   *
   * @return mixed
   */
  public static function search($value) {
    return array_search($value, self::toArray(), true);
  }

  /**
   * Returns a value when called statically like so: MyEnum::SOME_VALUE() given SOME_VALUE is a class constant
   *
   * @param string $name
   * @param array  $arguments
   *
   * @return static
   * @throws BadMethodCallException
   */
  public static function __callStatic($name, $arguments) {
    if (defined("static::$name")) {
      return new static(constant("static::$name"));
    }
    throw new BadMethodCallException("No static method or enum constant '$name' in class " . get_called_class());
  }
}