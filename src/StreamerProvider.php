<?php


namespace Prwnr\Streamer;

use Illuminate\Support\ServiceProvider;
use Prwnr\Streamer\Commands\FireCommand;
use Prwnr\Streamer\Commands\ListenCommand;
use Prwnr\Streamer\EventDispatcher\Streamer;
use Prwnr\Streamer\Redis\RedisVersion500;
use Predis\Profile\Factory;

/**
 * Class StreamerProvider
 * @package Prwnr\Streamer
 */
class StreamerProvider extends ServiceProvider
{

    /**
     * Register any application services.
     */
    public function register(): void
    {
        $this->offerPublishing();
        $this->configure();
        $this->registerRedisProfile();
        $this->registerCommands();
    }

    /**
     * Register the Redis 5.0 profile
     */
    private function registerRedisProfile(): void
    {
        Factory::define('5.0', RedisVersion500::class);
    }

    /**
     * Setup the configuration
     *
     * @return void
     */
    private function configure(): void
    {
        $this->mergeConfigFrom(
            __DIR__ . '/../config/streamer.php', 'streamer'
        );
    }

    /**
     * Setup the resource publishing groups
     *
     * @return void
     */
    private function offerPublishing(): void
    {
        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__ . '/../config/streamer.php' => app()->basePath('config/streamer.php'),
            ], 'config');
        }
    }


    /**
     * Register the Artisan commands.
     */
    private function registerCommands(): void
    {
        if ($this->app->runningInConsole()) {
            $this->commands([
                ListenCommand::class
            ]);
        }
    }
}