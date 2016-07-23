<?php

namespace mssql\Component;

class SerializerIgbinary implements SerializerInterface {

  /**
   * {@inheritdoc}
  */
  public function serialize($value) {
    return igbinary_serialize($value);
  }

  /**
   * {@inheritdoc}
   */
  public function unserialize($value) {
    return igbinary_unserialize($value);
  }
}
