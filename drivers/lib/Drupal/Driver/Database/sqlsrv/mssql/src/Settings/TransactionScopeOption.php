<?php

namespace mssql\Settings;

use mssql\Component\Enum;

class TransactionScopeOption extends Enum {
  const RequiresNew = 'RequiresNew';
  const Supress = 'Supress';
  const Required = 'Required';
}