<?php
/**
 * @copyright Copyright (c) 2021 Holger Hees <holger.hees@gmail.com>
 *
 * @license GNU AGPL version 3 or any later version
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

namespace OCA\Files_Home_INotify\Command;

use OC\DB\Connection;
use OC\DB\ConnectionAdapter;
use OC\Core\Command\Base;
use OC\Files\Filesystem;
use OCP\IUserManager;
use OCP\EventDispatcher\IEventDispatcher;
use OCP\Files\NotFoundException;
use OCP\Lock\LockedException;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class Notify extends Base {

	/** @var IUserManager $userManager */
	private $userManager;

	private $fd;

	private $pathMap = [];
	private $moveMap = [];
	private $userMap = [];
	
	public function __construct(IUserManager $userManager) 
	{
		$this->userManager = $userManager;
		parent::__construct();
	}

	protected function configure() 
	{
		parent::configure();

		$this
			->setName('files:notify')
			->setDescription('watch for file changes');
	}
	
	protected function execute(InputInterface $input, OutputInterface $output): int 
	{
        $this->fd = inotify_init();
        
        $users = $this->userManager->search('');
        foreach ($users as $user) 
        {
            $path = realpath($user->getHome());
            $this->userMap[ $path ] = $user;
            $this->register( $path . "/files/");
        }
        
        $this->listen($output);
        
        return 0;
	}
	
	private function getDirectoryIterator(string $path): \Iterator 
	{
		return new \RecursiveIteratorIterator(
			new \RecursiveDirectoryIterator($path,
				\FilesystemIterator::CURRENT_AS_PATHNAME + \FilesystemIterator::SKIP_DOTS),
			\RecursiveIteratorIterator::SELF_FIRST);
	}

	private function register($basePath): void 
	{
		$iterator = $this->getDirectoryIterator($basePath);

		$this->watchPath($basePath);
		foreach( $iterator as $path )
		{
			if (is_dir($path)) $this->watchPath($path);
		}
	}

	private function watchPath(string $path): void 
	{
		if ($this->fd === null) return;
		# The IN_MODIFY event is emitted on a file content change (e.g. via the write() syscall) while IN_CLOSE_WRITE occurs on closing the changed file. 
		# It means each change operation causes one IN_MODIFY event (it may occur many times during manipulations with an open file) whereas IN_CLOSE_WRITE is emitted only once (on closing the file). 
		$descriptor = inotify_add_watch($this->fd, $path, \IN_CLOSE_WRITE + \IN_CREATE + \IN_MOVED_FROM + \IN_MOVED_TO + \IN_DELETE);
		$this->pathMap[$descriptor] = $path;
        #error_log("watch " . $path);
	}
	
	protected function reconnectToDatabase(OutputInterface $output): Connection 
	{
		$connection = \OC::$server->get(Connection::class);
		try {
			$connection->close();
		} 
		catch (\Exception $ex) 
		{
			$output->writeln("<info>Error while disconnecting from database: {$ex->getMessage()}</info>");
		}
		while( !$connection->isConnected() ) 
		{
			try 
			{
				$connection->connect();
			}
			catch (\Exception $ex) 
			{
				$output->writeln("<info>Error while re-connecting to database: {$ex->getMessage()}</info>");
				sleep(60);
			}
		}
		return $connection;
	}
	
	private function readEvents(): array 
	{
    	if ($this->fd === null) return [];
		return inotify_read($this->fd);
    }
    
    private function processMove( $cookie ) 
    {
        if( !isset($this->moveMap[$cookie]['from']) || !isset($this->moveMap[$cookie]['to']) ) 
        {
            return;
        }
        
        $from = $this->moveMap[$cookie]['from'];
        $to = $this->moveMap[$cookie]['to'];
        foreach( $this->pathMap as $descriptor => $path )
        {
            if( strpos($path, $from) === 0 )
            {
                $this->pathMap[$descriptor] = str_replace($from,$to,$path);
            }
        }
        
        unset($this->moveMap[$cookie]);
    }
    
    private function processDelete($toProcess,$scanPath,$fullPath)
    {
                    
        // there is not need to scan subfolder, because they are deleted too
        foreach( array_keys($toProcess) as $_scanPath )
        {
            if( strpos($_scanPath, $scanPath) === 0 )
            {
                unset($toProcess[$_scanPath]);
            }
        }
        // clean the folder itself and all subfolders
        foreach( $this->pathMap as $descriptor => $path )
        {
            if( strpos($path, $fullPath) === 0 )
            {
                unset($this->pathMap[$descriptor]);
            }
        }
        
        return $toProcess;
    }
	
	private function processEvents(OutputInterface $output)
	{
        $connection = $this->reconnectToDatabase($output);
        $users = $this->userManager->search('');
        $scannerMap = [];
        foreach ($users as $user) 
        {
            $scannerMap[$user->getUID()] = new \OC\Files\Utils\Scanner(
                $user->getUID(),
                new ConnectionAdapter($connection),
                \OC::$server->query(IEventDispatcher::class),
                \OC::$server->getLogger()
            );	
        }
        
		$events = $this->readEvents();
		
        $toProcess = [];
        foreach ($events as $event) {
            $mask = $event["mask"];
            $name = $event["name"];
            $wd = $event["wd"];
            $path = $this->pathMap[$wd];
            
            $fullPath = $path . '/' . $name;
            $scanPath = null;

            if( $mask & \IN_ISDIR )
            {
                $cookie = $event['cookie'];
                if( ($mask & \IN_MOVED_TO) || ($mask & \IN_MOVED_FROM) )
                {
                    if( !isset($this->moveMap[$cookie]) )
                    {
                        $this->moveMap[$cookie] = [];
                    }
                }
            }
                       
            if( $mask & \IN_MOVED_FROM ) 
            {
                $scanPath = $path;
                if( $mask & \IN_ISDIR ) 
                {
                    $this->moveMap[$event['cookie']]['from'] = $fullPath;
                    $this->processMove($cookie);
                }
            }
            elseif ($mask & \IN_MOVED_TO) 
            {
                $scanPath = $path;
                if( $mask & \IN_ISDIR ) 
                {
                    $this->moveMap[$event['cookie']]['to'] = $fullPath;
                    $this->processMove($cookie);
                }
            }
            elseif ($mask & \IN_DELETE) 
            {
                $scanPath = $path;
                if( $mask & \IN_ISDIR ) 
                {
                    $toProcess = $this->processDelete($toProcess,$scanPath,$fullPath);
                }
            }
            elseif ($mask & \IN_CLOSE_WRITE) 
            {
                $scanPath = $path . '/' . $name;
            }
            elseif ($mask & \IN_CREATE) 
            {
                $scanPath = $path . '/' . $name;
                if( $mask & \IN_ISDIR ) 
                {
                    $this->register( $path);
                }
            }
            
            if( $scanPath != null )
            {
                $toProcess[$scanPath] = true;
            }
        }
        
        foreach( array_keys($toProcess) as $path )
        {
            try
            {
                $user = null;
                $scanner = null;
                foreach( $this->userMap as $basePath => $user )
                {
                    if( strpos($path, $basePath) === 0 )
                    { 
                        $path = $user->getUID() . substr($path,strlen($basePath));
                        $scanner = $scannerMap[$user->getUID()];
                        break;
                    }
                }          
                
                echo "File " . $path . " ...";
                $start = microtime(true);
                $scanner->scan($path, false, null);
                echo " processed in " . ( microtime(true) - $start ) . " ms\n";
            }
            catch( NotFoundException $e) 
            {
                echo " does not exists anymore\n";
			}
			catch( LockedException $e)
			{
                echo " locked\n";
			}
			catch (\Exception $e) 
			{
                echo " failed\n";
			}
        }
	}

	public function listen(OutputInterface $output): void 
	{
		if ($this->fd === null) return;

		stream_set_blocking($this->fd, true);
        pcntl_signal(SIGTERM, [$this, 'stop']);
		pcntl_signal(SIGINT, [$this, 'stop']);

		while( is_resource($this->fd) )
		{
            $read = [$this->fd];
			$write = null;
			$except = null;
			$changed = @stream_select($read, $write, $except, 60);
			if( $changed === false ) pcntl_signal_dispatch();

			if ($changed) $this->processEvents($output);
		}
	}

	public function stop(): void 
	{
		if ($this->fd === null) return;
		$fd = $this->fd;
		$this->fd = null;
		fclose($fd);
	}
}
