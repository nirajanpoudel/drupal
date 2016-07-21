#Wincache integration with Drupal

This Drupal modules provides implementations for several storage backends to leverage the power of Wincache and the COM_DOTNET class.

The module defines some dependencies in it's composer.json file, to make them work you need to rebuild
your drupal dependencies:

[Composer Manager for Drupal 8](https://www.drupal.org/node/2405811)

Simply run the **drupal composer-rebuild** or **drupal composer-update** commands.

##Motivation

Drupal and symfony heavily rely on in-memory storage to make things fast. Components such as the ClassLoader need in-memory storage to have an acceptable performance.

PHP on Windows is not production ready without the use of Wincache. When enabled, Wincache alters some low level PHP behaviour - such as file system manipulation functions - so that it performs properly on windows.

Recommended readings:

- http://php.net/manual/es/book.wincache.php
- http://www.iis.net/learn/application-frameworks/install-and-configure-php-on-iis/use-the-windows-cache-extension-for-php
- http://forums.iis.net/t/1201106.aspx?WinCache+1+3+5+release+on+SourceForge

##Installation

Download the Wincache PHP extension:

 http://windows.php.net/downloads/pecl/releases/

Enable it in your PHP.ini and configure it:
```
[PHP_WINCACHE]
extension=php_wincache.dll

wincache.ocenabled=1
wincache.fcenabled=1
wincache.ucenabled=1
wincache.fcachesize = 500
wincache.maxfilesize = 5000
wincache.ocachesize = 200
wincache.filecount = 50000
wincache.ttlmax = 2000
wincache.ucachesize = 150
wincache.scachesize = 85
wincache.ucenabled=On
wincache.srwlocks=1
wincache.reroute_enabled=Off
```

The Wincache module will automatically modify the servie container to:

- Use it as the fast backend in the cache.backend.chainedfast service.
- Use it as the fast backend in the cache.backend.superchainedfast service.
- Use it as the fast backend in the cache.rawbackend.chainedfast service.

Other optimizations will require to manually setup some things.

To enable the wincache FileCache replacement and class loader:

```php
if ($settings['hash_salt']) {
  $prefix = 'drupal.' . hash('sha256', 'drupal.' . $settings['hash_salt']);
  $loader = new \Symfony\Component\ClassLoader\WincacheClassLoader($prefix, $class_loader);
  unset($prefix);
  $class_loader->unregister();
  $loader ->register();
  $class_loader = $loader ;
}

$settings['file_cache']['default'] = [
    'class' => '\Drupal\Component\FileCache\FileCache',
    'cache_backend_class' => \Drupal\wincachedrupal\Cache\WincacheFileCacheBackend::class,
    'cache_backend_configuration' => [],
  ];
```

##.Net Framework Integration

The Wincache module exposes serveral services to make use the abilty of PHP to consume .Net code when
running on Windows in order to further improve performance.

Currently it uses the NetPhp runtime to acces the AjaxMin asset management library to provide
improved asset optimization performance.

To enable this integration:

- Enable the com_dotnet extension in your php.ini (extension=com_dotnet)
- Register the NetPhp runtime component
 - Download the NetPhp binaries from [here](http://www.drupalonwindows.com/sites/default/files/netphp2_0_0_4.zip)
 - Rename the netutilities_clr4.dll file to netutilities.dll
 - Register the component running the command: "C:\Windows\Microsoft.NET\Framework\v4.0.30319\regasm.exe" netutilities.dll /codebase
- Copy the AjaxMin.dll to /libraries/_bin/ajaxmin. You can download it from here: http://ajaxmin.codeplex.com/

Visit the site status page to make sure everything is working as expected.