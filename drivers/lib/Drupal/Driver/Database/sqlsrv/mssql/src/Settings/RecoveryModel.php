<?php

namespace mssql\Settings;

use \mssql\Component\Enum;

class RecoveryModel extends Enum {
  const Full = 'FULL';
  const BulkLogged = 'BULK_LOGGED';
  const Simple = 'SIMPLE';
}