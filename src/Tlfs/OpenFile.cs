using System.Collections.Generic;
using System;
using System.IO;

namespace Tlfs
{
    class OpenFile
    {
        private const int _blockSize = 1048576;
        private Drive _drive;
        private FileStream _tapeStream;
        private MemoryStream _bufferStream;
        public int CurrentFilePart { get; set; }
        public bool OpenForReading { get; set; }
        public bool Closed { get; set; }
        public Drive Drive
        {
            get
            {
                return _drive;
            }
        }
        public long Size;
        public long ReadOffset;
        public Dictionary<int, FilePartOnTape> FilePartsOnTape; //file part index, file part on tape ID
        
        //for writing
        public OpenFile(Drive drive, long firstFilePartOnTapeId)
        {
            FilePartsOnTape = new Dictionary<int, FilePartOnTape>();
            CurrentFilePart = 0;
            FilePartsOnTape.Add(CurrentFilePart, new FilePartOnTape(firstFilePartOnTapeId));
            _drive = drive;
            Size = 0;
            _bufferStream = new MemoryStream(_blockSize);
            OpenTapeStreamWrites();
            OpenForReading = false;
            Closed = false;
            ReadOffset = 0;
        }

        //for reading
        public OpenFile(Drive drive, Dictionary<int, FilePartOnTape> filePartsOnTape, long size)
        {
            FilePartsOnTape = filePartsOnTape;
            CurrentFilePart = 0;
            _drive = drive;
            Size = size;
            _tapeStream = new FileStream(drive.StPath, FileMode.Open, FileAccess.Read, FileShare.Read, _blockSize, false);
            var buffer = new byte[_blockSize];
            _tapeStream.Read(buffer);
            _bufferStream = new MemoryStream(buffer);
            OpenForReading = true;
            Closed = false;
            ReadOffset = 0;
        }

        ///Adds new data to the buffer but does not write the data to tape
        ///Returns the position of the buffer being written
        public int PrepareWrite(ReadOnlySpan<byte> buffer, int bufferPosition)
        {
            if (buffer.Length > _blockSize)
            {
                throw new Exception("Write is larger than the internal buffer, (todo).");
            }
            if (bufferPosition == 0)
            {
                //this is the initial write, which may run over the end of this block on tape
                if (_bufferStream.Position + buffer.Length > _blockSize)
                {
                    //write goes beyond the end of the buffer, write the bytes that fit, return the bytes remaining
                    var writeLength = (int)(_blockSize - _bufferStream.Position);
                    _bufferStream.Write(buffer.Slice(0, writeLength));
                    // bufferPosition = buffer.Length - writeLength;
                    return writeLength;
                }
                else
                {
                    //this write fits within the current bufferStream's remaining buffer
                    _bufferStream.Write(buffer);
                    return buffer.Length;
                }
            }
            else //write continues on from a previously half completed write
            {
                // var startPosition = buffer.Length - bufferPosition;
                _bufferStream.Write(buffer.Slice(bufferPosition));
                return buffer.Length;
            }
        }

        ///Flush the buffer to tape then clear the buffer
        ///Expect an IOException on end of tape and media write errors
        public void Flush()
        {
            _bufferStream.WriteTo(_tapeStream); //this may cause an ioexception
            _tapeStream.Flush(); //so may this
            _bufferStream.Close();
            _bufferStream = new MemoryStream(_blockSize);
        }

        public int Read(Span<byte> fuseBuffer)
        {
            if (fuseBuffer.Length > _blockSize)
            {
                throw new Exception("Read is larger than the internal buffer, (todo).");
            }
            if (_bufferStream.Position + fuseBuffer.Length > _blockSize)
            {
                //read goes beyond the end of the _bufferstream, read the remaining bytes, fill the _bufferstream with the next block and read the rest.
                var tempBuffer = new byte[fuseBuffer.Length];
                var bytesRead = _bufferStream.Read(tempBuffer, 0, fuseBuffer.Length);
                var remainingBytes = fuseBuffer.Length - bytesRead;
                _bufferStream.Close();
                var streamBuffer = new byte[_blockSize];
                var tapeBytesRead = _tapeStream.Read(streamBuffer);
                _bufferStream = new MemoryStream(streamBuffer);
                if (tapeBytesRead < remainingBytes)
                {
                    //end of file on tape, remaining bytes will fit into this read
                    remainingBytes = tapeBytesRead;
                }
                bytesRead += _bufferStream.Read(tempBuffer, bytesRead, remainingBytes);
                tempBuffer.CopyTo(fuseBuffer);
                return bytesRead;
            }
            else
            {
                return _bufferStream.Read(fuseBuffer);
            }
        }

        ///Any useful data in _bufferstream was written to the fuse buffer during the last normal read
        ///Previous to this, the _tapestream was opened on the start of the next file part
        ///The fuseBuffer may have some data in it from the previous normal read, continue on from there
        public int ContinueRead(Span<byte> fuseBuffer, int offset)
        {
            _bufferStream.Close();
            var streamBuffer = new byte[_blockSize];
            var tapeBytesRead = _tapeStream.Read(streamBuffer);
            _bufferStream = new MemoryStream(streamBuffer);
            var remainingBytes = fuseBuffer.Length - offset;
            var tempBuffer = fuseBuffer.ToArray();
            offset += _bufferStream.Read(tempBuffer, offset, remainingBytes);
            tempBuffer.CopyTo(fuseBuffer);
            return offset;
        }

        ///Expect an IOException on end of tape and media write errors
        public void CloseWrite()
        {
            _bufferStream.WriteTo(_tapeStream); //this may cause an ioexception
            _tapeStream.Flush(); //so may this
            _tapeStream.Close();
            _bufferStream.Close();
            Closed = true;
        }

        public void CloseRead()
        {
            // var extraBytesRead = _tapeStream.Read(new byte[_blockSize]);
            // if (extraBytesRead > 0)
            // {
            //     throw new Exception("Extra bytes were found at the end of the file on tape, this was unexpected.");
            // }
            _tapeStream.Close();
            _bufferStream.Close();
            Closed = true;
        }

        ///Used when changing tapes
        public void CloseTapeStream()
        {
            _tapeStream.Close();
        }

        ///Used when changing tapes
        public void OpenTapeStreamWrites()
        {
            _tapeStream = new FileStream(_drive.StPath, FileMode.Open, FileAccess.Write, FileShare.Write, _blockSize, false);
        }

        public void OpenTapeStreamReads()
        {
            _tapeStream = new FileStream(_drive.StPath, FileMode.Open, FileAccess.Read, FileShare.Read, _blockSize, false);
        }
    }
}