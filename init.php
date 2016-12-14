<?php
namespace Mod\Kafka;

use \DAutoloader;

DAutoloader::getInstance()->addNamespacePathArray(__NAMESPACE__,
    array(
        __DIR__.'/',
        __DIR__.'/model/',
    )
);
