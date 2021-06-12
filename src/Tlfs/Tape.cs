namespace Tlfs
{
    class Tape
    {
        public int Id { get; }
        public string Barcode { get; }
        public long Capacity { get; }
        public bool IsFull { get; set; }
        public bool MarkedForRemoval { get; set; }
        public int WriteErrors { get; set; }
        
        ///Used when loading from the database
        public Tape(int id, string barcode, long capacity, bool isFull, bool markedForRemoval, int writeErrors)
        {
            Id = id;
            Barcode = barcode;
            Capacity = capacity;
            IsFull = isFull;
            MarkedForRemoval = markedForRemoval;
            WriteErrors = writeErrors;
        }
    }
}
