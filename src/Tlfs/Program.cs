using System;
using System.IO;
using System.Threading.Tasks;
using Tmds.Fuse;
using System.Threading;
using System.Collections.Generic;

namespace Tlfs
{
    class Program
    {
        private static IFuseMount _mount;
        private static bool _abort = false;
        private static LibraryManager _libraryManager;

        /*
        * todo: fix zero byte file writes with dd
        */
        static async Task Main(string[] args)
        {
            try
            {
                var flags = new List<string>();
                flags.AddRange(args);
                var configLocation = "/etc/tlfs/tlfs.conf";
                var config = new Config(configLocation);
                try
                {
                    ValidateConfig(config);
                }
                catch (Exception e)
                {
                    if (e.Message == "invalid config")
                    {
                        Console.WriteLine("There was an issue while parsing the config file, aborting.");
                        return;
                    }
                    else
                    {
                        throw e;
                    }
                }
                if (!Fuse.CheckDependencies())
                {
                    Console.WriteLine(Fuse.InstallationInstructions);
                    return;
                }
                var db = new Database(config.PgUser, config.PgPass, config.PgHost);
                if (flags.Contains("--init"))
                {
                    //Initialise the database
                    db.InitDatabase(flags.Contains("--force"));
                    return;
                }
                if (!db.DbExists())
                {
                    Console.WriteLine($"Aborting startup, database tlfs does not exist. Create the database using the flag --init.");
                    return;
                }
                _libraryManager = new LibraryManager(db, config.DrivePaths, config.ChangerPath);
                var libraryManagerThread = new Thread(_libraryManager.LibraryManagerThread);
                try
                {
                    libraryManagerThread.Start();
                    var fileSystemManager = new FileSystemManager(db, _libraryManager);
                    var fuseFileSystem = new FuseFileSystem(db, fileSystemManager);
                    Log.Add(LogLevel.DEBUG, $"Mounting filesystem at {config.Mountpoint}");
                    Fuse.LazyUnmount(config.Mountpoint);
                    Directory.CreateDirectory(config.Mountpoint);
                    var mountOptions = new MountOptions(){SingleThread = false};
                    if (!_abort)
                    {
                        try
                        {
                            _mount = Fuse.Mount(config.Mountpoint, fuseFileSystem, mountOptions);
                            Log.Add(LogLevel.INFO, $"Filesystem is mounted at {config.Mountpoint}");
                            await _mount.WaitForUnmountAsync();
                            Log.Add(LogLevel.DEBUG, "Unmounting...");
                        }
                        catch (FuseException fe)
                        {
                            Console.WriteLine($"Fuse throw an exception: {fe}");
                            Console.WriteLine("Try unmounting the file system by executing:");
                            Console.WriteLine($"fuser -kM {config.Mountpoint}");
                            Console.WriteLine($"sudo umount -f {config.Mountpoint}");
                        }
                    }
                } catch (Exception e)
                {
                    Log.Add(LogLevel.ERROR, "There was an error during startup: " + e.Message);
                    Log.Add(LogLevel.ERROR, e.StackTrace);
                }
                finally
                {
                    RequestStop();
                }
            } catch (Exception e)
            {
                Log.Add(LogLevel.ERROR, "There was an error during startup: " + e.Message);
                Log.Add(LogLevel.ERROR, e.StackTrace);
            }
        }

        ///Triggered in case of unrecoverable or unexpected failure
        public static void Unmount()
        {
            if (_mount != null)
                _mount.LazyUnmount();
            _abort = true;
        }

        private static void RequestStop()
        {
            Log.Add(LogLevel.DEBUG, "Stopping worker threads.");
            if (_libraryManager != null)
                _libraryManager.RequestStop();
            Log.Add(LogLevel.DEBUG, "Worker threads stopped.");
        }

        private static void ValidateConfig(Config config)
        {
            if (config.Mountpoint == "" || config.Mountpoint.Length < 2)
            {
                Console.WriteLine($"Please configure a mount path in {config.ConfigLocation}");
                throw new Exception("invalid config");
            }
        }
    }
}
