using System.Collections.Generic;
using System.Threading;
using System;
using System.IO;
using System.Text.RegularExpressions;

namespace Tlfs
{
    public enum SlotType { DataTransferElement, StorageElement };
    public enum RequestType { NewWrite, NewRead, ContinueWrite, ContinueRead };

    class TapeLibrarySlot
    {
        public SlotType SlotType {get; set;}
        public int SlotNumber {get; set;}
        public bool Empty {get; set;}
        public string Barcode {get; set;}
    }

    class RobotRequest
    {
        public RequestType RequestType { get; set; }
        public bool Fulfilled { get; set; }
        public Drive Drive { get; set; }
        public int Timeout { get; set; }
        public int TapeId { get; set; }
    }

    class LibraryManager
    {
        private Database _db;
        private List<Drive> _drives;
        private LibraryRobot _robot;
        private Dictionary<int, RobotRequest> _robotRequests;
        private List<string> _tapeTrimRequests;
        private volatile bool _mounted = true;
        private bool _driveMappingComplete = false;

        public LibraryManager(Database db, List<string> DrivePaths, string ChangerPath)
        {
            Log.Add(LogLevel.DEBUG, "Library Manager initialising.");
            _db = db;
            _db.DeleteIncompleteFilesAndFilePartsOnTape();
            _drives = new List<Drive>();
            _robotRequests = new Dictionary<int, RobotRequest>();
            _tapeTrimRequests = new List<string>();
            DiscoverDevices();
            DiscoverTapes();
            _driveMappingComplete = true;
        }

        private void DiscoverDevices()
        {
            Log.Add(LogLevel.DEBUG, $"Discovering tape drives.");
            var deviceMatch = new Regex(@"^\[.*\]\s*(tape|mediumx)\s*.*\/dev\/(\w+)\s*(\/dev\/sg\d+)");
            var output = ProcessHelper.ExecProcess("/usr/bin/lsscsi", "--generic");
            foreach(var line in output.Split('\n'))
            {
                var matches = deviceMatch.Matches(line);
                if (matches.Count == 1)
                {
                    if (matches[0].Groups[1].Value == "tape")
                    {
                        _drives.Add(new Drive($"/dev/n{matches[0].Groups[2].Value}", matches[0].Groups[3].Value));
                    }
                    else if (matches[0].Groups[1].Value == "mediumx")
                    {
                        _robot = new LibraryRobot(matches[0].Groups[3].Value);
                    }
                }
            }
        }

        ///Runs as part of the constructor, triggered by Program
        ///Update the database with any newly discovered tapes
        private void DiscoverTapes()
        {
            Log.Add(LogLevel.DEBUG, "Looking for new tapes.");
            var tapes = _db.GetTapes();
            var sortedTapes = new Dictionary<string, Tape>();
            foreach(var tape in tapes)
            {
                sortedTapes.Add(tape.Barcode, tape);
            }
            foreach(var slot in _robot.Slots)
            {
                if (slot.Barcode != null && !slot.Barcode.StartsWith("CLN") && !sortedTapes.ContainsKey(slot.Barcode))
                {
                    _db.AddTape(slot.Barcode);
                }
            }
        }

        ///Runs in its own thread, triggered by Program
        ///Processes requests as they come in
        public void LibraryManagerThread()
        {
            try
            {
                Log.Add(LogLevel.DEBUG, "Library Manager ready.");
                Thread driveMapper = new Thread(MapDrivesInChanger);
                driveMapper.Start();
                while(_mounted)
                {
                    ProcessTapeTrimRequests();
                    ProcessRequests();
                    Thread.Sleep(100);
                }
                Log.Add(LogLevel.DEBUG, "Library Manager stopped.");
            }
            catch (Exception e)
            {
                Log.Add(LogLevel.ERROR, "Library Manager failed: " + e.Message);
                Log.Add(LogLevel.ERROR, e.StackTrace);
            }
            finally
            {
                Program.Unmount();
            }
        }

        ///processes and clears all tape trim requests
        private void ProcessTapeTrimRequests()
        {
            lock(_tapeTrimRequests)
            {
                foreach(var barcode in _tapeTrimRequests)
                {
                    bool skip = false;
                    //skip if the tape is in a busy drive
                    foreach(var drive in _drives)
                    {
                        //todo: update other functions so trim is performed once the drive is no longer busy
                        if (drive.Barcode == barcode && drive.Busy)
                        {
                            skip = true;
                        }
                    }
                    if (!skip)
                    {
                        _db.ClearDeletedFilePartsFromEndOfTape(barcode);
                    }
                }
                _tapeTrimRequests.Clear();
            }
        }

        ///Part of the LibraryManagerThread
        private void ProcessRequests()
        {
            var keys = new List<int>(_robotRequests.Keys);
            var requestToProcess = SelectRequestToProcess();
            //process the request
            if (requestToProcess != null)
            {
                switch(requestToProcess.RequestType)
                {
                    case RequestType.NewWrite:
                    {
                        HandleNewWriteRequest(requestToProcess);
                        break;
                    }
                    case RequestType.NewRead:
                    {
                        HandleNewReadRequest(requestToProcess);
                        break;
                    }
                    case RequestType.ContinueWrite:
                    {
                        HandleContinueWriteRequest(requestToProcess);
                        break;
                    }
                    case RequestType.ContinueRead:
                    {
                        HandleContinueReadRequest(requestToProcess);
                        break;
                    }
                }
            }
            //Tick the timeouts
            lock(_robotRequests)
            {
                var keysToRemove = new List<int>();
                foreach(var key in keys)
                {
                    if (_robotRequests[key].Timeout < 1)
                    {
                        keysToRemove.Add(key);
                        continue;
                    }
                    _robotRequests[key].Timeout--;
                }
                foreach(var key in keysToRemove)
                {
                    _robotRequests.Remove(key);
                }
            }
        }

        private RobotRequest SelectRequestToProcess()
        {
            lock(_robotRequests) //select the request to process
            {
                foreach(var request in _robotRequests.Values)
                {
                    if (request.Fulfilled)
                        continue;
                    if (request.RequestType == RequestType.ContinueRead)
                    {
                        return request;
                    }
                }
                foreach(var request in _robotRequests.Values)
                {
                    if (request.Fulfilled)
                        continue;
                    if (request.RequestType == RequestType.ContinueWrite)
                    {
                        return request;
                    }
                }
                foreach(var request in _robotRequests.Values)
                {
                    if (request.Fulfilled)
                        continue;
                    if (request.RequestType == RequestType.NewRead)
                    {
                        return request;
                    }
                }
                foreach(var request in _robotRequests.Values)
                {
                    if (request.Fulfilled)
                        continue;
                    if (request.RequestType == RequestType.NewWrite)
                    {
                        return request;
                    }
                }
            }
            return null;
        }

        private void HandleContinueReadRequest(RobotRequest requestToProcess)
        {
            _robot.EjectTape(requestToProcess.Drive.DataTransferElement);
            requestToProcess.Drive.Barcode = null;
            var tape = _db.GetTape(requestToProcess.TapeId);
            if (_robot.IsTapeInDriveSlot(tape.Barcode) && !_driveMappingComplete)
            {
                //drive mapping is still in progress, the tape may be in another drive but its status is unknown, skip for now and wait for the mapping to complete
                return;
            }
            foreach(var drive in _drives)
            {
                if (drive.Barcode == tape.Barcode)
                {
                    if (drive.Busy)
                    {
                        //the tape is in this drive but this drive is busy
                        return;
                    }
                    else
                    {
                        _robot.EjectTape(drive.DataTransferElement);
                    }
                }
            }
            _robot.MoveTapeToDrive(tape.Barcode, requestToProcess.Drive.DataTransferElement);
            requestToProcess.Drive.Barcode = tape.Barcode;
            requestToProcess.Fulfilled = true;
            Log.Add(LogLevel.DEBUG, $"Continue read request fulfilled using drive {requestToProcess.Drive.StPath} and tape {requestToProcess.Drive.Barcode}.");
            return;            
        }

        private void HandleContinueWriteRequest(RobotRequest requestToProcess)
        {
            _robot.EjectTape(requestToProcess.Drive.DataTransferElement);
            requestToProcess.Drive.Barcode = null;
            var tape = SelectTapeForWrite();
            if (tape != null) //if a tape is available
            {
                _robot.MoveTapeToDrive(tape.Barcode, requestToProcess.Drive.DataTransferElement);
                requestToProcess.Drive.Barcode = tape.Barcode;
            }
            if (requestToProcess.Drive.HasTape) //mark the request as fulfilled if a tape was found and inserted into the drive
            {
                requestToProcess.Fulfilled = true;
                Log.Add(LogLevel.DEBUG, $"Continue write request fulfilled using drive {requestToProcess.Drive.StPath} and tape {requestToProcess.Drive.Barcode}.");
                return;
            }
        }

        private void HandleNewReadRequest(RobotRequest requestToProcess)
        {
            var tape = _db.GetTape(requestToProcess.TapeId);
            if (_robot.IsTapeInDriveSlot(tape.Barcode) && !_driveMappingComplete)
            {
                //drive mapping is still in progress, the tape may be in another drive but its status is unknown, skip for now and wait for the mapping to complete
                return;
            }
            List<Drive> emptyDrives = new List<Drive>(); //no tape loaded and not busy
            List<Drive> fullDrives = new List<Drive>(); //tape is loaded and not busy
            foreach(var drive in _drives)
            {
                if (drive.Barcode == tape.Barcode)
                {
                    if (drive.Busy)
                    {
                        //the tape is in this drive but this drive is busy
                        return;
                    }
                    else
                    {
                        //the tape is in a drive and the drive is available
                        requestToProcess.Drive = drive;
                        requestToProcess.Drive.Busy = true;
                        requestToProcess.Fulfilled = true;
                        Log.Add(LogLevel.DEBUG, $"New read request fulfilled using drive {drive.StPath} and tape {drive.Barcode}.");
                        return;
                    }
                }
                else
                {
                    if (!drive.Busy)
                    {
                        if (drive.HasTape)
                        {
                            fullDrives.Add(drive);
                        }
                        else
                        {
                            emptyDrives.Add(drive);
                        }
                    }
                }
            }
            Drive availableDrive;
            if (emptyDrives.Count > 0)
            {
                availableDrive = emptyDrives[0];
            }
            else if (fullDrives.Count > 0)
            {
                //no empty drives but at least one full drive is available
                //eject the tape from the full drive and use that one
                availableDrive = fullDrives[0];
                _robot.EjectTape(availableDrive.DataTransferElement);
                availableDrive.Barcode = null;
            }
            else
            {
                //all drives are busy
                return;
            }
            _robot.MoveTapeToDrive(tape.Barcode, availableDrive.DataTransferElement);
            availableDrive.Barcode = tape.Barcode;
            requestToProcess.Drive = availableDrive;
            requestToProcess.Drive.Busy = true;
            requestToProcess.Fulfilled = true;
            Log.Add(LogLevel.DEBUG, $"New read request fulfilled using drive {availableDrive.StPath} and tape {availableDrive.Barcode}.");
            return;            
        }

        private void HandleNewWriteRequest(RobotRequest requestToProcess)
        {
            //select a free drive
            Drive availableDrive = null;
            foreach(var drive in _drives)
            {
                if (!drive.Busy)
                {
                    availableDrive = drive;
                    break;
                }
            }
            if (availableDrive != null)
            {
                if (availableDrive.HasTape) //eject if the tape is full
                {
                    var tape = _db.GetTape(availableDrive.Barcode);
                    if (tape.IsFull || tape.WriteErrors > 0)
                    {
                        _robot.EjectTape(availableDrive.DataTransferElement);
                        availableDrive.Barcode = null;
                    }
                }
                if (!availableDrive.HasTape) //get a tape if the drive is empty
                {
                    var tape = SelectTapeForWrite();
                    if (tape != null) //if a tape is available
                    {
                        _robot.MoveTapeToDrive(tape.Barcode, availableDrive.DataTransferElement);
                        availableDrive.Barcode = tape.Barcode;
                    }
                }
                if (availableDrive.HasTape) //mark the request as fulfilled if a tape was found and inserted into the drive
                {
                    requestToProcess.Drive = availableDrive;
                    requestToProcess.Drive.Busy = true;
                    requestToProcess.Fulfilled = true;
                    Log.Add(LogLevel.DEBUG, $"New write request fulfilled using drive {availableDrive.StPath} and tape {availableDrive.Barcode}.");
                    return;
                }
            }            
        }

        ///Select the tape in a tape slot with the least amount of deleted bytes
        private Tape SelectTapeForWrite()
        {
            Tape bestTape = null;
            long deletedBytes = long.MaxValue;
            foreach(var tape in _db.GetTapes())
            {
                if (_robot.IsTapeInDriveSlot(tape.Barcode))
                {
                    continue;
                }
                var selectedTapeDeletedBytes = _db.GetDeletedByteCountOnTape(tape.Barcode);
                if (selectedTapeDeletedBytes < deletedBytes)
                {
                    deletedBytes = selectedTapeDeletedBytes;
                    bestTape = tape;
                }

            }
            return bestTape;
        }

        public void RequestStop()
        {
            _mounted = false;
        }

        ///Part of the LibraryManagerThread
        ///Move tapes in and out of drives until each drive is associated with a changer slot
        private void MapDrivesInChanger()
        {
            try
            {
                Log.Add(LogLevel.DEBUG, "Mapping data transfer elements to linux drive paths.");
                lock(_robot)
                {
                    _robot.UpdateChangerStatus();
                    foreach(var slot in _robot.Slots)
                    {
                        if (!_mounted) return; //unmount has been triggered, abort
                        if (slot.SlotType == SlotType.DataTransferElement)
                        {
                            var drivesToCheck = new List<Drive>(); //the drives to check for changes
                            if (slot.Empty) //put a tape in and look for drives that went from empty to full
                            {
                                foreach (var drive in _drives)
                                {
                                    lock(drive)
                                    {
                                        if (drive.DataTransferElement != -1) continue; //skip as it's already identified
                                        drive.UpdateDriveStatus();
                                        if (!drive.HasTape)
                                        {
                                            //element is empty and this drive is empty, maybe the are they same drive?
                                            drivesToCheck.Add(drive);
                                        }
                                    }
                                }
                                if (!_robot.MoveAnyTapeToDriveSlot(slot.SlotNumber))
                                {
                                    throw new Exception("Could not find a spare tape to move into the drive.");
                                }
                                bool found = false;
                                foreach (var drive in drivesToCheck)
                                {
                                    lock(drive)
                                    {
                                        drive.UpdateDriveStatus();
                                        if (drive.HasTape)
                                        {
                                            //we can assume the tape was moved into this drive, so this slot belongs to this drive
                                            drive.DataTransferElement = slot.SlotNumber;
                                            drive.Barcode = _robot.DriveSlots[drive.DataTransferElement].Barcode;
                                            drive.Busy = false;
                                            Log.Add(LogLevel.DEBUG, $"Drive at slot {drive.DataTransferElement} is mapped to {drive.StPath} and contains tape {drive.Barcode}.");
                                            found = true;
                                            break;
                                        }
                                    }
                                }
                                if (!found)
                                {
                                    Log.Add(LogLevel.INFO, $"Could not find a drive mapping for drive at slot {slot.SlotNumber}.");
                                }
                            }
                            else
                            {
                                //eject a tape and look for drives that went from full to empty
                                foreach (var drive in _drives)
                                {
                                    lock(drive)
                                    {
                                        if (drive.DataTransferElement != -1) continue; //skip as it's already identified
                                        drive.UpdateDriveStatus();
                                        if (drive.HasTape)
                                        {
                                            //element is full and this drive is full, maybe the are they same drive?
                                            drivesToCheck.Add(drive);
                                        }
                                    }
                                }
                                _robot.EjectTape(slot.SlotNumber);
                                bool found = false;
                                foreach (var drive in drivesToCheck)
                                {
                                    lock(drive)
                                    {
                                        drive.UpdateDriveStatus();
                                        if (!drive.HasTape)
                                        {
                                            //we can assume the tape was ejected from this drive, so this slot belongs to this drive
                                            drive.DataTransferElement = slot.SlotNumber;
                                            drive.Barcode = _robot.DriveSlots[drive.DataTransferElement].Barcode;
                                            drive.Busy = false;
                                            Log.Add(LogLevel.DEBUG, $"Drive at slot {drive.DataTransferElement} is mapped to {drive.StPath}.");
                                            found = true;
                                            break;
                                        }
                                    }
                                }
                                if (!found)
                                {
                                    Log.Add(LogLevel.INFO, $"Could not find a drive mapping for drive at slot {slot.SlotNumber}.");
                                }
                            }
                        }
                    }
                    foreach(var drive in _drives)
                    {
                        lock(drive)
                        {
                            if (drive.DataTransferElement == -1)
                            {
                                Log.Add(LogLevel.INFO, $"Could not find a drive mapping for drive at path {drive.StPath}.");
                            }
                        }
                    }
                    Log.Add(LogLevel.DEBUG, "Drive mapping is complete.");
                }
            }
            catch (Exception e)
            {
                Log.Add(LogLevel.ERROR, "Drive mapping failed: " + e.Message);
                Log.Add(LogLevel.ERROR, e.StackTrace);
                Program.Unmount();
            }
        }

        ///Runs as part of the fuse threads
        ///Triggered by fuse upon opening a file for write
        public void LoadDriveForWrite(int threadId, long entryId, out long firstFilePartOnTapeId, out Drive drive)
        {
            /*
            create a new request to load the drive for a new write operation
            return once the request has been fulfilled, otherwise, refresh the timeout
            */
            Log.Add(LogLevel.INFO, $"Loading drive for new write.");
            firstFilePartOnTapeId = -1;
            drive = null;
            lock(_robotRequests)
            {
                if (_robotRequests.ContainsKey(threadId))
                {
                    _robotRequests.Remove(threadId);
                    Log.Add(LogLevel.INFO, $"A previous request already existed for this thread, it was removed.");
                }
                _robotRequests.Add(threadId, new RobotRequest()
                {
                    Fulfilled = false,
                    RequestType = RequestType.NewWrite,
                    Timeout = 60
                });
            }
            Log.Add(LogLevel.INFO, $"Waiting for drive to become available...");
            while(drive == null)
            {
                lock(_robotRequests)
                {
                    if (_robotRequests[threadId].Fulfilled)
                    {
                        if (_robotRequests[threadId].Drive == null)
                        {
                            throw new Exception("There was an issue loading the drive.");
                        }
                        drive = _robotRequests[threadId].Drive;
                        _robotRequests.Remove(threadId);
                    }
                    else
                    {
                        _robotRequests[threadId].Timeout = 60;
                    }
                }
                Thread.Sleep(100);
            }
            bool reloadRequired;
            do
            {   //if writing the start marker fails, we'll need a new tape
                reloadRequired = false;
                Log.Add(LogLevel.INFO, $"Drive has become available, winding tape to the correct position.");
                _db.ClearDeletedFilePartsFromEndOfTape(drive.Barcode);
                var lastPartExists = _db.GetLastFilePartOnTape(drive.Barcode, out int lastPartTapeIndex, out long lastPartId);
                int filePartTapeIndex;
                if (lastPartExists)
                {
                    drive.VerifyMarker('e', lastPartId, lastPartTapeIndex + 2);
                    filePartTapeIndex = lastPartTapeIndex + 3;
                }
                else
                {
                    drive.WindToStart();
                    filePartTapeIndex = 0;
                }
                firstFilePartOnTapeId = _db.SaveNewFilePartOnTape(entryId, drive.Barcode, filePartTapeIndex, 0);
                try
                {
                    drive.WriteMarker('s', firstFilePartOnTapeId);
                }
                catch(IOException e)
                {
                    //new tape required, create a request and wait
                    Log.Add(LogLevel.DEBUG, $"Message: {e.Message}");
                    if (e.Message == "No space left on device")
                    {
                        Log.Add(LogLevel.DEBUG, "End of tape found, loading next tape.");
                        _db.MarkTapeAsFull(drive.Barcode);
                    }
                    reloadRequired = true;
                    _db.DeleteFilePartOnTape(firstFilePartOnTapeId);
                    HandleWriteIOException(drive);
                    lock(_robotRequests)
                    {
                        _robotRequests.Add(threadId, new RobotRequest()
                        {
                            Fulfilled = false,
                            RequestType = RequestType.NewWrite,
                            Drive = drive,
                            Timeout = 60
                        });
                    }
                    bool ready = false;
                    while(!ready)
                    {
                        lock(_robotRequests)
                        {
                            if (_robotRequests[threadId].Fulfilled)
                            {
                                if (_robotRequests[threadId].Drive == null)
                                {
                                    throw new Exception("There was an issue loading the drive.");
                                }
                                ready = true;
                                _robotRequests.Remove(threadId);
                            }
                            else
                            {
                                _robotRequests[threadId].Timeout = 60;
                            }
                        }
                        Thread.Sleep(100);
                    }
                }
            } while (reloadRequired);
            Log.Add(LogLevel.INFO, $"Drive is ready to be written to.");
        }

        ///Creates a request to load the tape for a continued write
        ///and waits for the request to be completed
        ///Triggered by:
        ///  FileSystemManager.ReleaseFile when flushing the buffer to tape fails
        public void LoadDriveToContinueWrite(int threadId, long entryId, int filePartNumber, out long nextFilePartOnTapeId, Drive drive)
        {
            //a file is already open and a drive has already been selected
            Log.Add(LogLevel.DEBUG, "Loading drive to continue write.");
            lock(_robotRequests)
            {
                if (_robotRequests.ContainsKey(threadId))
                {
                    _robotRequests.Remove(threadId);
                    Log.Add(LogLevel.INFO, $"A previous request already existed for this thread, it was removed.");
                }
                _robotRequests.Add(threadId, new RobotRequest()
                {
                    Fulfilled = false,
                    RequestType = RequestType.ContinueWrite,
                    Drive = drive,
                    Timeout = 60
                });
            }
            Log.Add(LogLevel.INFO, $"Waiting for drive to become available...");
            bool requestFulfilled = false;
            while (!requestFulfilled)
            {
                lock(_robotRequests)
                {
                    if (_robotRequests[threadId].Fulfilled)
                    {
                        requestFulfilled = true;
                        _robotRequests.Remove(threadId);
                    }
                    else
                    {
                        _robotRequests[threadId].Timeout = 60;
                    }
                }
                Thread.Sleep(100);
            }
            bool reloadRequired;
            do
            {   //if writing the start marker fails, we'll need a new tape
                reloadRequired = false;
                Log.Add(LogLevel.INFO, $"Drive has become available, winding tape to the correct position.");
                _db.ClearDeletedFilePartsFromEndOfTape(drive.Barcode);
                var lastPartExists = _db.GetLastFilePartOnTape(drive.Barcode, out int lastPartTapeIndex, out long lastPartId);
                int filePartTapeIndex;
                if (lastPartExists)
                {
                    drive.VerifyMarker('e', lastPartId, lastPartTapeIndex + 2);
                    filePartTapeIndex = lastPartTapeIndex + 3;
                }
                else
                {
                    drive.WindToStart();
                    filePartTapeIndex = 0;
                }
                nextFilePartOnTapeId = _db.SaveNewFilePartOnTape(entryId, drive.Barcode, filePartTapeIndex, filePartNumber);
                try
                {
                    drive.WriteMarker('s', nextFilePartOnTapeId);
                }
                catch(IOException e)
                {
                    //new tape required, create a request and wait
                    Log.Add(LogLevel.DEBUG, $"Message: {e.Message}");
                    if (e.Message == "No space left on device")
                    {
                        Log.Add(LogLevel.DEBUG, "End of tape found, loading next tape.");
                        _db.MarkTapeAsFull(drive.Barcode);
                    }
                    reloadRequired = true;
                    _db.DeleteFilePartOnTape(nextFilePartOnTapeId);
                    HandleWriteIOException(drive);
                    lock(_robotRequests)
                    {
                        _robotRequests.Add(threadId, new RobotRequest()
                        {
                            Fulfilled = false,
                            RequestType = RequestType.NewWrite,
                            Drive = drive,
                            Timeout = 60
                        });
                    }
                    bool ready = false;
                    while(!ready)
                    {
                        lock(_robotRequests)
                        {
                            if (_robotRequests[threadId].Fulfilled)
                            {
                                if (_robotRequests[threadId].Drive == null)
                                {
                                    throw new Exception("There was an issue loading the drive.");
                                }
                                ready = true;
                                _robotRequests.Remove(threadId);
                            }
                            else
                            {
                                _robotRequests[threadId].Timeout = 60;
                            }
                        }
                        Thread.Sleep(100);
                    }
                }
            } while (reloadRequired);
            Log.Add(LogLevel.INFO, $"Drive is ready to be written to.");
        }

        public void HandleWriteIOException(Drive drive)
        {
            //todo: make this work in more scenarios
            // drive.UpdateTapeAlertStatus();
            // if (drive.AlertWriteFailure)
            // {
            //     _db.IncrementWriteErrorCount(drive.Barcode);
            // }
        }

        public void LoadDriveForRead(int threadId, long entryId, Dictionary<int, FilePartOnTape> filePartsOnTape, out Drive drive)
        {
            drive = null;
            Log.Add(LogLevel.INFO, $"Loading drive for new read.");
            lock(_robotRequests)
            {
                if (_robotRequests.ContainsKey(threadId))
                {
                    _robotRequests.Remove(threadId);
                    Log.Add(LogLevel.INFO, $"A previous request already existed for this thread, it was removed.");
                }
                _robotRequests.Add(threadId, new RobotRequest()
                {
                    Fulfilled = false,
                    RequestType = RequestType.NewRead,
                    Timeout = 60,
                    TapeId = filePartsOnTape[0].TapeId
                });
            }
            Log.Add(LogLevel.INFO, $"Waiting for drive to become available...");
            while(drive == null)
            {
                lock(_robotRequests)
                {
                    if (_robotRequests[threadId].Fulfilled)
                    {
                        if (_robotRequests[threadId].Drive == null)
                        {
                            throw new Exception("There was an issue loading the drive.");
                        }
                        drive = _robotRequests[threadId].Drive;
                        _robotRequests.Remove(threadId);
                    }
                    else
                    {
                        _robotRequests[threadId].Timeout = 60;
                    }
                }
                Thread.Sleep(100);
            }
            Log.Add(LogLevel.INFO, $"Drive has become available, winding tape to the correct position.");
            _db.ClearDeletedFilePartsFromEndOfTape(drive.Barcode);
            drive.VerifyMarker('s', filePartsOnTape[0].Id, filePartsOnTape[0].TapeIndex);
            Log.Add(LogLevel.INFO, $"Drive is ready to be read.");
        }

        public void LoadDriveToContinueRead(int threadId, FilePartOnTape currentFilePartOnTape, Drive drive)
        {
            Log.Add(LogLevel.INFO, $"Loading drive for to continue read.");
            lock(_robotRequests)
            {
                if (_robotRequests.ContainsKey(threadId))
                {
                    _robotRequests.Remove(threadId);
                    Log.Add(LogLevel.INFO, $"A previous request already existed for this thread, it was removed.");
                }
                _robotRequests.Add(threadId, new RobotRequest()
                {
                    Fulfilled = false,
                    RequestType = RequestType.ContinueRead,
                    Timeout = 60,
                    Drive = drive,
                    TapeId = currentFilePartOnTape.TapeId
                });
            }
            Log.Add(LogLevel.INFO, $"Waiting for drive to become available...");
            bool requestFulfilled = false;
            while(!requestFulfilled)
            {
                lock(_robotRequests)
                {
                    if (_robotRequests[threadId].Fulfilled)
                    {
                        requestFulfilled = true;
                        _robotRequests.Remove(threadId);
                    }
                    else
                    {
                        _robotRequests[threadId].Timeout = 60;
                    }
                }
                Thread.Sleep(100);
            }
            Log.Add(LogLevel.INFO, $"Drive has become available, winding tape to the correct position.");
            _db.ClearDeletedFilePartsFromEndOfTape(drive.Barcode);
            drive.VerifyMarker('s', currentFilePartOnTape.Id, currentFilePartOnTape.TapeIndex);
            Log.Add(LogLevel.INFO, $"Drive is ready to be read.");
        }

        ///Submit a tape trim request to the queue, don't wait for it to finish
        public void TrimTape(int tapeId)
        {
            var barcode = _db.GetTapeBarcode(tapeId);
            Log.Add(LogLevel.DEBUG, $"Requesting trim of tape {barcode}.");
            lock(_tapeTrimRequests)
            {
                if (!_tapeTrimRequests.Contains(barcode))
                {
                    _tapeTrimRequests.Add(barcode);
                }
            }
        }
    }
}