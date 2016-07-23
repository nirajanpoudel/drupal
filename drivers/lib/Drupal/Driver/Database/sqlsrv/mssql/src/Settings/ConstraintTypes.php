<?php

namespace mssql\Settings;

use \mssql\Component\Enum;

/**
 * Constraint types for SQL Server.
 */
class ConstraintTypes extends Enum {
  /**
   * CHECK constraint
   */
  const CCHECK = 'C';

  /**
   * DEFAULT (constraint or stand-alone)
   */
  const CDEFAULT = 'D';

  /**
   * FOREIGN KEY constraint
   */
  const CFOREIGNKEY = 'F';

  /**
   * PRIMARY KEY constraint
   */
  const CPRIMARYKEY = 'P';

  /**
   * Rule (old-style, stand-alone)
   */
  const CRULE = 'R';

  /**
   * UNIQUE constraint
   */
  const CUNIQUE = 'UQ';
}