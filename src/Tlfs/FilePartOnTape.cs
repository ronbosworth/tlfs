namespace Tlfs
{
    class FilePartOnTape
    {
        public long Id { get; }
        public int TapeId { get; }
        public long FileId { get; }
        public long BlocksWritten { get; }
        public int BlockSize { get; }
        public int TapeIndex { get; }

        ///Load from the database for reads
        public FilePartOnTape(long id, int tapeId, long fileId, long blocksWritten, int blockSize, int tapeIndex)
        {
            Id = id;
            TapeId = tapeId;
            FileId = fileId;
            BlocksWritten = blocksWritten;
            BlockSize = blockSize;
            TapeIndex = tapeIndex;
        }

        ///Created during writes
        public FilePartOnTape(long id)
        {
            Id = id;
        }
    }
}