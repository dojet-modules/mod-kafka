<?php
/**
 * Filename: ModuleKafka.class.php
 *
 * @author liyan
 * @since 2016 12 14
 */
namespace Mod\Kafka;

use \BaseModule;

class ModuleKafka extends BaseModule {

    public function setZookeeper($zookeeper) {
        self::config('zookeeper', $zookeeper);
    }

}
