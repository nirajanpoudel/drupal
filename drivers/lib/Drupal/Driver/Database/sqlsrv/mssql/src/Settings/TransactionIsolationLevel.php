<?php

namespace mssql\Settings;

use mssql\Component\Enum;

/**
 * Available transaction isolation levels for MSSQL.
 */
class TransactionIsolationLevel extends Enum {
  const ReadUncommitted = 'READ UNCOMMITTED';
  const ReadCommitted = 'READ COMMITTED';
  const RepeatableRead = 'REPEATABLE READ';
  const Snapshot = 'SNAPSHOT';
  const Serializable = 'SERIALIZABLE';
  const Chaos = 'CHAOS';
  const Ignore = 'IGNORE';
}