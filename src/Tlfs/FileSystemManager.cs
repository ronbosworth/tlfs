using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System;
using System.IO;
using static Tmds.Linux.LibC;

namespace Tlfs
{
    class FileSystemManager
    {
        private Database _db;
        private LibraryManager _libraryManager;
        private Dictionary<string, Entry> _staticEntries;
        private Dictionary<long, OpenFile> _openFiles;
        private Dictionary<int, AutoResetEvent> _waitHandles; //for queueing reads and writes
        public FileSystemManager(Database db, LibraryManager libraryManager)
        {
            _db = db;
            _libraryManager = libraryManager;
            _openFiles = new Dictionary<long, OpenFile>();
            _staticEntries = new Dictionary<string, Entry>();
            _staticEntries.Add("/", new Entry(-1, 0b111_101_101, true));
            _waitHandles = new Dictionary<int, AutoResetEvent>();
        }

        public Entry GetEntry(string path)
        {
            if (_staticEntries.ContainsKey(path))
            {
                return _staticEntries[path];
            }
            else
            {
                return _db.GetEntry(path);
            }
        }

        public bool TryOpenFile(string path, bool truncate, out long entryId)
        {
            var entry = GetEntry(path);
            entryId = entry.Id;
            Log.Add(LogLevel.DEBUG, $"Opening {path}, file handle {entryId}.");
            if (entry == null) return false;
            if (entry.IsDirectory) return false;
            // If the file is already open, wait for it to become available
            bool alreadyOpen = true;
            while (alreadyOpen)
            {
                lock(_openFiles)
                {
                    if (!_openFiles.ContainsKey(entry.Id))
                    {
                        //reload the entry from the database in case it was already open and may have changed since then
                        entry = GetEntry(path);
                        if (entry == null) return false;
                        if (entry.IsDirectory) return false;
                        _openFiles.Add(entry.Id, null);
                        alreadyOpen = false;
                        Log.Add(LogLevel.DEBUG, $"File {path} added to open files list.");
                    }
                }
                if (alreadyOpen)
                {
                    Thread.Sleep(100);
                }
            }
            if (truncate)
            {
                //todo: delete any data on tape associated with this file
                //set the size of this file to zero in the database
                entry.Size = 0;
            }
            if (entry.Size == 0) //open for writing
            {
                Log.Add(LogLevel.DEBUG, $"File {path} has a size of 0, open for write implied.");
                Drive drive;
                long firstFilePartOnTapeId;
                //ask the tape library to load a tape and validate the position, ready to start a new write
                _libraryManager.LoadDriveForWrite(Thread.CurrentThread.ManagedThreadId, entry.Id, out firstFilePartOnTapeId, out drive);
                var openFile = new OpenFile(drive, firstFilePartOnTapeId);
                lock(_openFiles)
                {
                    _openFiles[entry.Id] = openFile;
                }
            }
            else
            {
                Log.Add(LogLevel.DEBUG, $"File {path} size larger than 0, open for read implied.");
                //open for reading
                Drive drive;
                var filePartsOnTape = _db.GetFilePartsOnTape(path);
                //ask the tape library to load a tape and validate the position, ready to start a new read
                _libraryManager.LoadDriveForRead(Thread.CurrentThread.ManagedThreadId, entry.Id, filePartsOnTape, out drive);
                var openFile = new OpenFile(drive, filePartsOnTape, entry.Size);
                lock(_openFiles)
                {
                    _openFiles[entry.Id] = openFile;
                }
            }
            return true;
        }

        public void ReleaseFile(long fileHandle)
        {
            /*
            * when the file was opened for write:
            *  a drive was marked as busy and a tape was selected
            *  the tape was moved to the correct position and the marker was verified
            *  a new openfile entry was added to _openFiles
            *
            * during file write:
            *  the buffer was written to
            *  and when the buffer was full, that buffer was written to tape
            *  and if the tape became full, a new tape was selected, moved, verified
            *  
            * on file close:
            *  if the file size is 0, mark the file part on tape as deleted
            *  otherwise:
            *    flush the remaining buffer to tape
            *    write the end of file marker to tape
            *  update the database with the new file size and write complete status
            *  remove the openfile entry from _openfiles
            *  mark the tape drive as no longer busy
            */

            /* 
            * when the file was opened for read... todo
            */
            OpenFile openFile;
            lock(_openFiles)
            {
                if (_openFiles.ContainsKey(fileHandle))
                {
                    openFile = _openFiles[fileHandle];
                }
                else
                {
                    Log.Add(LogLevel.DEBUG, $"Fuse asked for a file to be released but it was either never opened or has been released already. File handle: {fileHandle}");
                    return;
                }
            }
            Log.Add(LogLevel.DEBUG, $"Releasing file handle {fileHandle}.");
            if (openFile.OpenForReading)
            {
                openFile.CloseRead();
            }
            else
            {
                bool closeComplete = false;
                while (!closeComplete)
                {
                    try
                    {
                        openFile.CloseWrite(); //attempt to flush the buffer to tape, if this fails with an io exception, request a new tape and try again
                        closeComplete = true;
                    }
                    catch (IOException e)
                    {
                        Log.Add(LogLevel.DEBUG, $"Message: {e.Message}");
                        if (e.Message == "No space left on device")
                        {
                            Log.Add(LogLevel.DEBUG, "End of tape found, loading next tape.");
                            _db.MarkTapeAsFull(openFile.Drive.Barcode);
                        }
                        _libraryManager.HandleWriteIOException(openFile.Drive);
                        openFile.CloseTapeStream();
                        openFile.CurrentFilePart++;
                        long nextFilePartOnTapeId;
                        _libraryManager.LoadDriveToContinueWrite(Thread.CurrentThread.ManagedThreadId, fileHandle, openFile.CurrentFilePart, out nextFilePartOnTapeId, openFile.Drive);
                        openFile.FilePartsOnTape[openFile.CurrentFilePart] = new FilePartOnTape(nextFilePartOnTapeId);
                        openFile.OpenTapeStreamWrites();
                    }
                }
                if (openFile.Size == 0)
                {
                    Log.Add(LogLevel.DEBUG, $"Zero byte file, deleting.");
                    _db.MarkFilePartOnTapeAsDeleted(openFile.FilePartsOnTape[0].Id);
                    _db.DeleteEntry(fileHandle);
                    _db.ClearDeletedFilePartsFromEndOfTape(openFile.Drive.Barcode);
                }
                else
                {
                    try
                    {
                        openFile.Drive.WriteMarker('e', openFile.FilePartsOnTape[openFile.CurrentFilePart].Id);
                    }
                    catch (IOException e)
                    {
                        Log.Add(LogLevel.DEBUG, $"Message: {e.Message}");
                        if (e.Message == "No space left on device")
                        {
                            Log.Add(LogLevel.DEBUG, "End of tape found, aborting the end of file mark.");
                            _db.MarkTapeAsFull(openFile.Drive.Barcode);
                        }
                        _libraryManager.HandleWriteIOException(openFile.Drive);
                    }
                    _db.UpdateEntryWriteComplete(fileHandle, openFile.Size);
                }
            }
            openFile.Drive.Busy = false;
            lock(_openFiles)
            {
                _openFiles.Remove(fileHandle);
            }
            Log.Add(LogLevel.DEBUG, $"Release complete for file handle {fileHandle}.");
            foreach (var waitHandle in _waitHandles.Values)
            {
                waitHandle.Set();
            }
        }

        public void Write(long fileHandle, long offset, ReadOnlySpan<byte> buffer)
        {
            /*
            * Write to the buffer
            * While the write goes beyond the end of the buffer:
            *  flush the buffer to tape
            *  if the tape is full
            *    mark the file part on tape as write complete
            *    request a new tape, create a new file part and retry the write
            * todo: store the number of blocks written and block size in the db
            */
            OpenFile openFile;
            lock(_openFiles)
            {
                openFile = _openFiles[fileHandle];
            }
            if (openFile.Size != offset)
            {
                throw new Exception("Out of order write encountered, aborting.");
            }
            int bufferPosition = openFile.PrepareWrite(buffer, 0);
            var remainingBytes = buffer.Length - bufferPosition;
            if (remainingBytes > 0) //a flush to tape is required
            {
                bool flushComplete = false;
                while (!flushComplete)
                {
                    try
                    {
                        openFile.Flush(); //this may fail with an io exception
                        flushComplete = true;
                    }
                    catch (IOException e)
                    {
                        Log.Add(LogLevel.DEBUG, $"Message: {e.Message}");
                        if (e.Message == "No space left on device")
                        {
                            Log.Add(LogLevel.DEBUG, "End of tape found, loading next tape.");
                            _db.MarkTapeAsFull(openFile.Drive.Barcode);
                        }
                        _libraryManager.HandleWriteIOException(openFile.Drive);
                        openFile.CloseTapeStream();
                        openFile.CurrentFilePart++;
                        long nextFilePartOnTapeId;
                        _libraryManager.LoadDriveToContinueWrite(Thread.CurrentThread.ManagedThreadId, fileHandle, openFile.CurrentFilePart, out nextFilePartOnTapeId, openFile.Drive);
                        Log.Add(LogLevel.DEBUG, "Continuing write.");
                        openFile.FilePartsOnTape[openFile.CurrentFilePart] = new FilePartOnTape(nextFilePartOnTapeId);
                        openFile.OpenTapeStreamWrites();
                    }
                }
                //write the remainder to the tape buffer
                openFile.PrepareWrite(buffer, bufferPosition);
            }
            openFile.Size += buffer.Length;
        }

        //Expect reads to be triggered similtaniously and out of order
        public int Read(long fileHandle, long offset, Span<byte> buffer)
        {
            OpenFile openFile;
            lock(_openFiles)
            {
                openFile = _openFiles[fileHandle];
            }
            while (true)
            {
                lock(openFile)
                {
                    if (openFile.ReadOffset == offset)
                    {
                        break;
                    }
                }
                AutoResetEvent waitHandle;
                // Log.Add(LogLevel.DEBUG, "Out of order read encountered, sleeping.");
                lock(_waitHandles)
                {
                    if (!_waitHandles.TryGetValue(Thread.CurrentThread.ManagedThreadId, out waitHandle))
                    {
                        waitHandle = new AutoResetEvent(false);
                        _waitHandles.Add(Thread.CurrentThread.ManagedThreadId, waitHandle);
                    }
                }
                waitHandle.WaitOne(1000);
                // Log.Add(LogLevel.DEBUG, "Awake.");
            }
            //Read requests leave the above gate in order
            // Log.Add(LogLevel.DEBUG, $"Read starting offset {offset}.");
            if (openFile.Closed)
            {
                Log.Add(LogLevel.DEBUG, $"File is now closed, Was the read aborted?");
                return 0;
            }
            var bytesRead = openFile.Read(buffer);
            if (bytesRead < buffer.Length)
            {
                Log.Add(LogLevel.DEBUG, $"End of file on tape found.");
                if (openFile.ReadOffset == openFile.Size)
                {
                    Log.Add(LogLevel.DEBUG, $"End of file found.");
                }
                else
                {
                    Log.Add(LogLevel.DEBUG, $"File read is not complete, load next tape.");
                    openFile.CloseTapeStream();
                    openFile.CurrentFilePart++;
                    _libraryManager.LoadDriveToContinueRead(Thread.CurrentThread.ManagedThreadId, openFile.FilePartsOnTape[openFile.CurrentFilePart], openFile.Drive);
                    Log.Add(LogLevel.DEBUG, "Continuing read.");
                    openFile.OpenTapeStreamReads();
                    bytesRead = openFile.ContinueRead(buffer, bytesRead);
                }
            }
            // Log.Add(LogLevel.DEBUG, $"Read complete offset {offset} bytes read {bytesRead}.");
            Interlocked.Add(ref openFile.ReadOffset, bytesRead);
            lock(_waitHandles)
            {
                foreach(var waitHandle in _waitHandles.Values)
                {
                    waitHandle.Set();
                }
            }
            return bytesRead;
        }

        ///Mark the file, and the file parts as deleted
        ///Queue a trim for each tape where data was stored
        public int TryDeleteFile(string path)
        {
            var entry = _db.GetEntry(path);
            if (entry == null)
            {
                return -ENOENT;
            }
            if (entry.IsDirectory)
            {
                return -EISDIR;
            }
            var filePartsOnTape = _db.GetFilePartsOnTape(path);
            _db.MarkEntryAsDeleted(entry.Id);
            var tapes = new List<int>();
            foreach(var part in filePartsOnTape)
            {
                if (!tapes.Contains(part.Value.TapeId))
                {
                    tapes.Add(part.Value.TapeId);
                }
                _db.MarkFilePartOnTapeAsDeleted(part.Value.Id);
            }
            foreach(var tapeId in tapes)
            {
                _libraryManager.TrimTape(tapeId);
            }
            return 0;
        }
    }
}