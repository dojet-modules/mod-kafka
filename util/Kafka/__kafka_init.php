<?php
spl_autoload_register(function($className)
{
    $basePath = __DIR__.'/../';
    $classFile = $basePath . str_replace('\\', DIRECTORY_SEPARATOR, $className) . '.php';
    if (function_exists('stream_resolve_include_path')) {
        $file = stream_resolve_include_path($classFile);
    } else {
        $file = false;
        foreach (explode(PATH_SEPARATOR, get_include_path()) as $path) {
            var_dump($path . '/' . $classFile);
            if (file_exists($path . '/' . $classFile)) {
                $file = $path . '/' . $classFile;
                break;
            }
        }
    }
    /* If file is found, store it into the cache, classname <-> file association */
    if (($file !== false) && ($file !== null)) {
        include_once $file;
        return;
    }

    throw new RuntimeException($className. ' not found');
});
