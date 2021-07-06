# files_home_inotify

Adds support detecting changes in all local user home folder with occ files:notify

## Requirements

This app uses the php inotify extensions which is required to be installed before this app can be enabled.
The php inotify extension can be installed from your distribution's package or [pecl](https://pecl.php.net/package/inotify).

## Usage

To detect changes you need to run the `files:notify`.

Note that this command runs continuously and should be started in the background by an init system or other task manager for best usage. 

Find the id of the external storage that should be checked

Run the filesystem watch

```
occ files:notify
```

## Scalability notes

Due to the nature of `inotify` the memory requirements of listening for changes 
scales linearly with the number of folders in the storage.

Additionally it's required to configure `fs.inotify.max_user_watches` on the server
to be higher than the total number of folders being watched.
 
