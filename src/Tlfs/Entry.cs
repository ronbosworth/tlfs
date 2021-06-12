using System;
using System.Linq;
namespace Tlfs
{
    class Entry
    {
        public long Id { get; set; }
        public int Uid { get; set; }
        public int Gid { get; set; }
        public int Mode { get; set; }
        public DateTime Accessed { get; set; }
        public DateTime Modified { get; set; }
        public long ParentId { get; set; }
        public bool IsDirectory { get; set; }
        public long Size { get; set; }

        public Entry(long id, int mode, bool isDirectory)
        {
            Id = id;
            IsDirectory = isDirectory;
            Uid = 0;
            Gid = 0;
            Mode = mode;
            Accessed = DateTime.UtcNow;
            Modified = DateTime.UtcNow;
            ParentId = 0;
            Size = 0;
        }

        public Entry(int mode, bool isDirectory, long id, int uid, int gid, DateTime accessed, DateTime modified, long parentId, long size)
        {
            Id = id;
            IsDirectory = isDirectory;
            Uid = uid;
            Gid = gid;
            Mode = mode;
            Accessed = accessed;
            Modified = modified;
            ParentId = parentId;
            Size = size;
        }
    }
}