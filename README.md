# kobo-srv

This is a small, zero-configuration utility to sync a folder of epubs to a kobo ereader.

You can run it with a command like: `kobo-srv path/to/epubs`. It'll answer any requests sent to it by a kobo device, incrementally pushing files as they're added to the folder.

To configure your reader to use it, first mount your reader as a hard drive, then edit `.kobo/Kobo/Kobo eReader.conf`. Change or add the following line:

```ini
api_endpoint=http://<ip:port>
```

Then reboot and sync.
