using System;
using System.Text.RegularExpressions;
using System.IO;
using System.Text;

namespace Tlfs
{
    class Drive
    {
        //todo wrap all tape moves with ioexception catches
        private readonly int _blockSize = 1048576;
        private bool _hasTape;
        private string _barcode;
        private string _sgPath;
        // private bool _alertWriteFailure;
        // public bool AlertWriteFailure 
        // {
        //     get
        //     {
        //         return _alertWriteFailure;
        //     }
        // }
        public string StPath { get; }
        public int DataTransferElement { get; set; }
        public bool HasTape
        {
            get
            {
                return _hasTape;
            }
        }
        public bool Busy { get; set; }
        public string Barcode
        {
            get
            {
                return _barcode;
            }
            set
            {
                _barcode = value;
                UpdateDriveStatus();
            }
        }
        
        public Drive(string stPath, string sgPath)
        {
            StPath = stPath;
            _sgPath = sgPath;
            DataTransferElement = -1;
            Busy = true;
        }

        public void UpdateDriveStatus()
        {
            Log.Add(LogLevel.DEBUG, $"Updating drive status for drive at path {StPath}.");
            var output = ProcessHelper.ExecProcess("/usr/bin/mt", $"-f {StPath} status");
            _hasTape = output.Contains("ONLINE");
        }

        // ///Checks the sg logs for any tape alerts
        // public void UpdateTapeAlertStatus()
        // {
        //     _alertWriteFailure = false;
        //     Log.Add(LogLevel.DEBUG, $"Updating drive status for drive at path {StPath}.");
        //     var output = ProcessHelper.ExecProcess("/usr/bin/sg_logs", $"{_sgPath} --page=0x2e");
        //     foreach(var line in output.Split('\n'))
        //     {
        //         if(line.Contains(":"))
        //         {
        //             var lineParts = line.Trim().Split(':');
        //             if (lineParts[1] == "1")
        //             {
        //                 Log.Add(LogLevel.ERROR, $"Tape alert discovered: {line.Trim()}");
        //                 if (lineParts[0] == "Write failure")
        //                 {
        //                     _alertWriteFailure = true;
        //                 }
        //             }
        //         }
        //     }
        // }

        public void WindToStart()
        {
            if (_hasTape)
            {
                Log.Add(LogLevel.DEBUG, $"Rewinding the tape in drive {StPath} to file number 0.");
                var output = ProcessHelper.ExecProcess("/usr/bin/mt", $"-f {StPath} rewind");
            }
        }

        //rewinds or fast forwards the tape to the start of the File Number, ready for reading or writing
        private void WindTapeToIndex(int index)
        {
            Log.Add(LogLevel.DEBUG, $"Moving tape to file number {index}.");
            if (index == 0)
            {
                WindToStart();
            }
            var (currentFileNumber, currentBlockNumber) = GetTapeCurrentIndex();
            if (currentBlockNumber != 0)
            {
                //this can happen if the previous read was block aligned, try reading another block first
                using (var fileStream = new FileStream(StPath, FileMode.Open, FileAccess.Read, FileShare.Read, _blockSize, false))
                {
                    var extraBytesRead = fileStream.Read(new byte[_blockSize]);
                }
            }
            (currentFileNumber, currentBlockNumber) = GetTapeCurrentIndex();
            if (currentBlockNumber != 0)
            {
                Log.Add(LogLevel.DEBUG, $"Block Number is at {currentBlockNumber} when it should be 0, rewinding to the start.");
                WindToStart();
                (currentFileNumber, currentBlockNumber) = GetTapeCurrentIndex();
                if (currentBlockNumber != 0)
                {
                    throw new Exception("Could not rewind the tape to the correct position.");
                }
            }
            if (currentFileNumber < index)
            {
                //fast forward
                ProcessHelper.ExecProcess("/usr/bin/mt", $"-f {StPath} fsf {index - currentFileNumber}");
                (currentFileNumber, currentBlockNumber) = GetTapeCurrentIndex();
                if (index != currentFileNumber)
                {
                    throw new Exception("Could not fast forward the tape to the correct position.");
                }
            }
            else if (currentFileNumber > index)
            {
                //rewind
                ProcessHelper.ExecProcess("/usr/bin/mt", $"-f {StPath} bsfm {currentFileNumber - index + 1}");
                (currentFileNumber, currentBlockNumber) = GetTapeCurrentIndex();
                if (index != currentFileNumber)
                {
                    throw new Exception("Could not rewind the tape to the correct position.");
                }
            }
            Log.Add(LogLevel.DEBUG, $"Tape is at file number {index}");
        }

        public (int fileNumber, int blockNumber) GetTapeCurrentIndex()
        {
            var indexMatch = new Regex(@"^File number=(-?\d+), block number=(-?\d+), partition=(-?\d+).$");
            var output = ProcessHelper.ExecProcess("/usr/bin/mt", $"-f {StPath} status");
            foreach(var line in output.Split('\n'))
            {
                var matches = indexMatch.Matches(line);
                if (matches.Count == 1)
                {
                    int blockNumber;
                    int fileNumber;
                    if (!int.TryParse(matches[0].Groups[1].Value, out fileNumber))
                    {
                        break;
                    }
                    if (!int.TryParse(matches[0].Groups[2].Value, out blockNumber))
                    {
                        break;
                    }
                    return (fileNumber, blockNumber);
                }
            }
            throw new Exception("Could not find the tape's current file or block number.");
        }

        //Return true if the marker matches what was expected
        //todo verify file hash and file name and the end file marker
        public void VerifyMarker(char type, long id, int index)
        {
            //Move the tape to the correct position
            WindTapeToIndex(index);
            Byte[] buffer = new byte[_blockSize];
            int bytesRead;
            int extraBytesRead;
            using (var fileStream = new FileStream(StPath, FileMode.Open, FileAccess.Read, FileShare.Read, _blockSize, false))
            {
                bytesRead = fileStream.Read(buffer);
                extraBytesRead = fileStream.Read(new byte[_blockSize]);
            }
            if (extraBytesRead != 0)
            {
                Log.Add(LogLevel.INFO, $"Extra bytes ({extraBytesRead}) were found while verifying marker, this is unexpected");
            }
            else
            {
                using (var memoryStream = new MemoryStream(buffer))
                {
                    BinaryReader reader = new BinaryReader(memoryStream, Encoding.UTF8);
                    var readType = reader.ReadChar();
                    var readBlockOnTapeId = reader.ReadInt64();
                    Log.Add(LogLevel.DEBUG, $"Verified: {readType == type && readBlockOnTapeId == id} bytesRead: {bytesRead} readType: {readType} readBlockOnTapeId: {readBlockOnTapeId}");
                }
            }
        }

        //writes a structure to tape that can be used to verify the tape is at the correct position
        public void WriteMarker(char type, long blockOnTapeId)
        {
            Log.Add(LogLevel.DEBUG, $"Writing end of file marker for block on tape id {blockOnTapeId}.");
            Byte[] buffer = new byte[_blockSize];
            using (var memoryStream = new MemoryStream(buffer))
            {            
                BinaryWriter writer = new BinaryWriter(memoryStream, Encoding.UTF8);
                writer.Write(type);
                writer.Write(blockOnTapeId);
            }
            using (var fileStream = new FileStream(StPath, FileMode.Open, FileAccess.Write, FileShare.Write, _blockSize, false))
            {
                fileStream.Write(buffer);
            }
        }
    }
}
