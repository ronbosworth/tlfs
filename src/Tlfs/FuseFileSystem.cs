using System;
using System.Text;
using Tmds.Fuse;
using Tmds.Linux;
using static Tmds.Linux.LibC;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Collections.Concurrent;

namespace Tlfs
{
    class FuseFileSystem : FuseFileSystemBase
    {
        public override bool SupportsMultiThreading => true;
        private Database _db;
        private FileSystemManager _fileSystem;

        public FuseFileSystem(Database db, FileSystemManager fileSystem)
        {
            Log.Add(LogLevel.DEBUG, "Main thread init");
            _db = db;
            _fileSystem = fileSystem;
            Log.Add(LogLevel.DEBUG, _db.GetVersion());
        }

        public override int GetAttr(ReadOnlySpan<byte> path, ref stat stat, FuseFileInfoRef fiRef)
        {
            try
            {
                Log.Add(LogLevel.DEBUG, $"GetAttr path:{Encoding.UTF8.GetString(path)}");
                var entry = _fileSystem.GetEntry(Encoding.UTF8.GetString(path));
                if (entry == null)
                {
                    Log.Add(LogLevel.DEBUG, $"Entry does not exist path:{Encoding.UTF8.GetString(path)}");
                    return -ENOENT;
                }
                stat.st_atim = entry.Accessed.ToTimespec();
                stat.st_mtim = entry.Modified.ToTimespec();
                stat.st_gid = (uint)entry.Gid;
                stat.st_uid = (uint)entry.Uid;
                stat.st_nlink = 1;
                if (entry.IsDirectory)
                {
                    stat.st_mode = S_IFDIR | (ushort)entry.Mode;
                    stat.st_nlink++; // add additional link for self ('.')
                }
                else
                {
                    stat.st_mode = S_IFREG | (ushort)entry.Mode;
                    stat.st_size = entry.Size;
                    stat.st_blocks = entry.Size / 512;
                }
                return 0;
            } catch (Exception e)
            {
                Log.Add(LogLevel.ERROR, e.Message);
                Log.Add(LogLevel.ERROR, e.StackTrace);
                throw e;
            }
        }

        public override int Read(ReadOnlySpan<byte> path, ulong offset, Span<byte> buffer, ref FuseFileInfo fi)
        {
            try
            {
                // Log.Add(LogLevel.DEBUG, $"Read path:{Encoding.UTF8.GetString(path)} File id: {fi.fh} Offset: {offset} Buffer Length: {buffer.Length}");
                return _fileSystem.Read((long)fi.fh, (long)offset, buffer);
            } catch (Exception e)
            {
                Log.Add(LogLevel.ERROR, e.Message);
                Log.Add(LogLevel.ERROR, e.StackTrace);
                throw e;
            }
        }
        
        public override int Write(ReadOnlySpan<byte> path, ulong offset, ReadOnlySpan<byte> buffer, ref FuseFileInfo fi)
        {
            try
            {
                // Log.Add(LogLevel.DEBUG, $"Write path:{Encoding.UTF8.GetString(path)} File id: {(long)fi.fh} Offset: {offset} Buffer Length: {buffer.Length}");
                _fileSystem.Write((long)fi.fh, (long)offset, buffer);
                return buffer.Length;
            } catch (Exception e)
            {
                Log.Add(LogLevel.ERROR, e.Message);
                Log.Add(LogLevel.ERROR, e.StackTrace);
                throw e;
            }
        }

        ///Mark the file as write complete
        ///If the file has a size of 0, delete the database entry for the file part on tape
        ///Mark the drive as not busy
        public override void Release(ReadOnlySpan<byte> path, ref FuseFileInfo fi)
        {
            try
            {
                Log.Add(LogLevel.DEBUG, $"Release path:{Encoding.UTF8.GetString(path)}");
                _fileSystem.ReleaseFile((long)fi.fh); //todo verify this cast is ok
            } catch (Exception e)
            {
                Log.Add(LogLevel.ERROR, e.Message);
                Log.Add(LogLevel.ERROR, e.StackTrace);
                throw e;
            }
        }

        //todo https://github.com/Mortal/python-statfs
        //https://stackoverflow.com/questions/4965355/converting-statvfs-to-percentage-free-correctly
        public override int StatFS(ReadOnlySpan<byte> path, ref statvfs statfs)
        {
            statfs.f_blocks = (ulong)(_db.GetTotalEstimatedTapeCapacity() / 1024); //total blocks: displays as size (capacity of all tapes)
            statfs.f_bfree = statfs.f_blocks - (ulong)(_db.GetTotalConsumedSpace() / 1024); //blocks free: displays as used (total size of all files on tape)
            statfs.f_bavail = statfs.f_blocks - (ulong)(_db.GetTotalUsefulConsumedSpace() / 1024); //blocks available: displays as available (estimated remaining capacity available on all tapes)
            statfs.f_bsize = 1024; //block size
            statfs.f_frsize = 1024; //block size
            statfs.f_files = (ulong)long.MaxValue; //max inodes
            statfs.f_favail =  statfs.f_files - (ulong)(_db.GetMaxFileId());//inodes available: displays as can be used (total remaining db IDs left)
            statfs.f_ffree = statfs.f_favail; //inodes free: displays as actual used
            // statfs.f_flag = 0;
            // statfs.f_fsid = 0;
            statfs.f_namemax = 255; //maximum filename length
            return 0;
        }

        public override int ChMod(ReadOnlySpan<byte> path, mode_t mode, FuseFileInfoRef fiRef)
        {
            try
            {
                Log.Add(LogLevel.DEBUG, $"ChMod path:{Encoding.UTF8.GetString(path)}");
                var entry = _db.GetEntry(Encoding.UTF8.GetString(path));
                if (entry == null)
                {
                    return -ENOENT;
                }
                _db.UpdateEntryMode(entry.Id, (int)mode);
                return 0;
            } catch (Exception e)
            {
                Log.Add(LogLevel.ERROR, e.Message);
                Log.Add(LogLevel.ERROR, e.StackTrace);
                throw e;
            }
        }

        public override int MkDir(ReadOnlySpan<byte> path, mode_t mode)
        {
            try
            {
                Log.Add(LogLevel.DEBUG, $"MkDir path:{Encoding.UTF8.GetString(path)}");
                return CreateEntry(Encoding.UTF8.GetString(path), (int)mode, true, out _);
            } catch (Exception e)
            {
                Log.Add(LogLevel.ERROR, e.Message);
                Log.Add(LogLevel.ERROR, e.StackTrace);
                throw e;
            }
        }

        public override int ReadDir(ReadOnlySpan<byte> path, ulong offset, ReadDirFlags flags, DirectoryContent content, ref FuseFileInfo fi)
        {
            try
            {
                Log.Add(LogLevel.DEBUG, $"ReadDir path:{Encoding.UTF8.GetString(path)}");
                content.AddEntry(".");
                content.AddEntry("..");
                List<string> names;
                names = _db.GetDirectoryContents(_fileSystem.GetEntry(Encoding.UTF8.GetString(path)).Id);
                foreach(var name in names)
                {
                    content.AddEntry(name);
                }
                return 0;
            } catch (Exception e)
            {
                Log.Add(LogLevel.ERROR, e.Message);
                Log.Add(LogLevel.ERROR, e.StackTrace);
                throw e;
            }
        }
        
        public override int Create(ReadOnlySpan<byte> path, mode_t mode, ref FuseFileInfo fi)
        {
            try
            {
                Log.Add(LogLevel.DEBUG, $"Create path:{Encoding.UTF8.GetString(path)}.");
                long fileHandle;
                var output = CreateEntry(Encoding.UTF8.GetString(path), (int)mode, false, out fileHandle);
                if (output == 0)
                {
                    fi.fh = (ulong)fileHandle;
                }
                Log.Add(LogLevel.DEBUG, $"Create path:{Encoding.UTF8.GetString(path)} complete.");
                return output;
            } catch (Exception e)
            {
                Log.Add(LogLevel.ERROR, e.Message);
                Log.Add(LogLevel.ERROR, e.StackTrace);
                throw e;
            }
        }

        private int CreateEntry(string path, int mode, bool isDirectory, out long fileHandle)
        {
            fileHandle = -1;
            var parentPath = path.Substring(0, path.LastIndexOf('/'));
            if (parentPath == "")
            {
                parentPath = "/";
            }
            var parentEntry = _fileSystem.GetEntry(parentPath);
            if (parentEntry == null)
            {
                return -ENOENT;
            }
            if (!parentEntry.IsDirectory)
            {
                return -ENOTDIR;
            }
            if (_fileSystem.GetEntry(path) != null)
            {
                return -EEXIST;
            }
            _db.SaveNewEntry(path, (int)mode, parentEntry.Id, isDirectory);
            Log.Add(LogLevel.DEBUG, $"New entry created {path}.");
            if (!isDirectory) ///Open after creation if it's a file
            {
                var opened = _fileSystem.TryOpenFile(path, false, out fileHandle);
                if (!opened) { return -ENOENT; }
            }
            return 0;
        }

        public override int RmDir(ReadOnlySpan<byte> path)
        {
            try
            {
                Log.Add(LogLevel.DEBUG, $"RmDir path:{Encoding.UTF8.GetString(path)}");
                var entry = _db.GetEntry(Encoding.UTF8.GetString(path));
                if (entry == null)
                {
                    return -ENOENT;
                }
                if (!entry.IsDirectory)
                {
                    return -ENOTDIR;
                }
                if (_db.GetDirectoryContents(_fileSystem.GetEntry(Encoding.UTF8.GetString(path)).Id).Count > 0)
                {
                    return -ENOTEMPTY;
                }
                _db.MarkEntryAsDeleted(entry.Id);
                return 0;
            } catch (Exception e)
            {
                Log.Add(LogLevel.ERROR, e.Message);
                Log.Add(LogLevel.ERROR, e.StackTrace);
                throw e;
            }
        }

        public override int Open(ReadOnlySpan<byte> path, ref FuseFileInfo fi)
        {
            try
            {
                Log.Add(LogLevel.DEBUG, $"Open path:{Encoding.UTF8.GetString(path)}.");
                if ((fi.flags & O_TRUNC) != 0)
                {
                    //todo open for writing
                    return -EROFS;
                }
                long fileHandle;
                var opened = _fileSystem.TryOpenFile(Encoding.UTF8.GetString(path), false, out fileHandle);
                if (opened)
                {
                    fi.fh = (ulong)fileHandle;
                    Log.Add(LogLevel.DEBUG, $"Open path:{Encoding.UTF8.GetString(path)} complete.");
                    return 0;
                }
                else
                {
                    Log.Add(LogLevel.DEBUG, $"Open path:{Encoding.UTF8.GetString(path)} failed, could not find the entry.");
                    return -ENOENT;
                }
            } catch (Exception e)
            {
                Log.Add(LogLevel.ERROR, e.Message);
                Log.Add(LogLevel.ERROR, e.StackTrace);
                throw e;
            }
        }
        
        public override int Unlink(ReadOnlySpan<byte> path)
        {
            try
            {
                Log.Add(LogLevel.DEBUG, $"Unlink path:{Encoding.UTF8.GetString(path)}");
                //mark the file in the database as deleted
                //queue a trim operation for this tape
                return _fileSystem.TryDeleteFile(Encoding.UTF8.GetString(path));
            } catch (Exception e)
            {
                Log.Add(LogLevel.ERROR, e.Message);
                Log.Add(LogLevel.ERROR, e.StackTrace);
                throw e;
            }
        }

        public override int UpdateTimestamps(ReadOnlySpan<byte> path, ref timespec atime, ref timespec mtime, FuseFileInfoRef fiRef)
        {
            try
            {
                Log.Add(LogLevel.DEBUG, $"UpdateTimestamps path:{Encoding.UTF8.GetString(path)} atime.tv_sec, tv_nsec: {atime.tv_sec}, {atime.tv_nsec} mtime.tv_sec, tv_nsec: {mtime.tv_sec}, {mtime.tv_nsec}");
                var entry = _db.GetEntry(Encoding.UTF8.GetString(path));
                if (entry == null)
                {
                    return -ENOENT;
                }
                DateTime accessed = DateTime.UtcNow;
                DateTime modified = DateTime.UtcNow;
                DateTime now = atime.IsNow() || mtime.IsNow() ? DateTime.UtcNow : DateTime.MinValue;
                if (!atime.IsOmit())
                {
                    accessed = atime.IsNow() ? now : atime.ToDateTime();
                }
                if (!mtime.IsOmit())
                {
                    modified = mtime.IsNow() ? now : mtime.ToDateTime();
                }
                _db.UpdateTimeStamps(entry.Id, accessed, modified);
                return 0;
            } catch (Exception e)
            {
                Log.Add(LogLevel.ERROR, e.Message);
                Log.Add(LogLevel.ERROR, e.StackTrace);
                throw e;
            }
        }

        public override int Rename(ReadOnlySpan<byte> path, ReadOnlySpan<byte> newPath, int flags)
        {
            try
            {
                Log.Add(LogLevel.DEBUG, $"Rename path:{Encoding.UTF8.GetString(path)} to {Encoding.UTF8.GetString(newPath)} flags: {flags}");
                if (flags != 0)
                {
                    return -EINVAL;
                }
                var newEntry = _db.GetEntry(Encoding.UTF8.GetString(newPath));
                if (newEntry != null)
                {
                    //file already exists in that location, delete that file out of the way
                    var result = _fileSystem.TryDeleteFile(Encoding.UTF8.GetString(path));
                    if (result != 0)
                    {
                        return result;
                    }
                }
                var entry = _db.GetEntry(Encoding.UTF8.GetString(path));
                if (entry == null)
                {
                    return -ENOENT;
                }
                _db.RenameEntry(entry.Id, Encoding.UTF8.GetString(newPath));
                return 0;
            } catch (Exception e)
            {
                Log.Add(LogLevel.ERROR, e.Message);
                Log.Add(LogLevel.ERROR, e.StackTrace);
                throw e;
            }
        }

        public override int Chown(ReadOnlySpan<byte> path, uint uid, uint gid, FuseFileInfoRef fiRef)
        {
            try
            {
                Log.Add(LogLevel.DEBUG, $"chown path:{Encoding.UTF8.GetString(path)} uid:{uid} gid:{gid}");
                var entry = _db.GetEntry(Encoding.UTF8.GetString(path));
                if (entry == null)
                {
                    return -ENOENT;
                }
                _db.UpdateEntryOwner(entry.Id, (int)uid, (int)gid);
                return 0;
            } catch (Exception e)
            {
                Log.Add(LogLevel.ERROR, e.Message);
                Log.Add(LogLevel.ERROR, e.StackTrace);
                throw e;
            }
        }
    }
}