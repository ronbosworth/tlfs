using System.Collections.Generic;
using Npgsql;
using System;
using System.Text;
using System.Threading;
using System.Linq;

namespace Tlfs
{
    class Database
    {
        private readonly string _pgHost;
        private readonly string _pgUser;
        private readonly string _pgPass;
        public Database(string pgUser, string pgPass, string pgHost)
        {
            _pgHost = pgHost;
            _pgUser = pgUser;
            _pgPass = pgPass;
        }

        public bool DbExists()
        {
            var sql = "";
            bool exists;
            using (var initialCon = new NpgsqlConnection($"Host={_pgHost};Username={_pgUser};Password={_pgPass};"))
            {
                sql = "SELECT 1 from pg_database WHERE datname='tlfs';";
                using (var cmd = new NpgsqlCommand(sql, initialCon))
                {
                    initialCon.Open();
                    exists = !(cmd.ExecuteScalar() is null);
                    initialCon.Close();
                }
            }
            return exists;
        }

        ///<summary>Creates a new database, deletes the existing one if forced.</summary>
        public void InitDatabase(bool force)
        {
            var sql = "";
            using (var initialCon = new NpgsqlConnection($"Host={_pgHost};Username={_pgUser};Password={_pgPass};"))
            {
                if (DbExists())
                {
                    if (force)
                    {
                        Log.Add(LogLevel.DEBUG, "Deleting existing database tlfs due to forced initialisation.");
                        sql = "DROP DATABASE tlfs";
                        using (var cmd = new NpgsqlCommand(sql, initialCon))
                        {
                            initialCon.Open();
                            cmd.ExecuteNonQuery();
                            initialCon.Close();
                        }
                    }
                    else
                    {
                        throw new Exception("Existing database discovered during initialisation, use the flag --force to delete and recreate this database.");
                    }
                }
                Log.Add(LogLevel.DEBUG, "Creating database tlfs");
                sql = "CREATE DATABASE tlfs WITH OWNER = postgres ENCODING = 'UTF8' CONNECTION LIMIT = -1;";
                using (var cmd = new NpgsqlCommand(sql, initialCon))
                {
                    initialCon.Open();
                    cmd.ExecuteNonQuery();
                    initialCon.Close();
                }
            }
            CreateTables();
        }

        private void CreateTables()
        {
            var sql = "CREATE TABLE file_entry " +
                "( " +
                    "id BIGSERIAL NOT NULL, " +
                    "path TEXT NOT NULL, " +
                    "mode INTEGER NOT NULL, " +
                    "is_directory BOOLEAN NOT NULL, " +
                    "is_deleted BOOLEAN NOT NULL, " +
                    "parent_id BIGINT NOT NULL, " +
                    "accessed TIMESTAMP NOT NULL, " +
                    "modified TIMESTAMP NOT NULL, " +
                    "gid INTEGER NOT NULL, " +
                    "uid INTEGER NOT NULL, " +
                    "size BIGINT NOT NULL, " + 
                    "write_complete BOOLEAN NOT NULL, " +
                    "file_hash TEXT NOT NULL, " +
                    "PRIMARY KEY (id) " +
                "); " +
                "CREATE INDEX file_entry_name_idx ON file_entry(\"path\"); " +
                "CREATE INDEX file_entry_parent_id_idx ON file_entry(\"parent_id\"); " +
                "CREATE INDEX file_entry_is_deleted_idx ON file_entry(\"is_deleted\"); " +
                "CREATE TABLE tape " +
                "( " +
                    "id SERIAL NOT NULL, " +
                    "barcode TEXT NOT NULL, " +
                    "capacity BIGINT NOT NULL, " +
                    "is_full BOOLEAN NOT NULL, " +
                    "marked_for_removal BOOLEAN NOT NULL, " +
                    "write_errors INTEGER NOT NULL, " +
                    "PRIMARY KEY (id) " +
                "); " +
                "CREATE INDEX tape_barcode_idx ON tape(\"barcode\"); " +
                "CREATE TABLE file_part_on_tape " +
                "( " +
                    "id BIGSERIAL NOT NULL, " +
                    "tape_id INTEGER NOT NULL, " +
                    "file_id BIGINT NOT NULL, " +
                    "file_part_number INTEGER NOT NULL, " +
                    "blocks_written BIGINT NOT NULL, " +
                    "block_size INTEGER NOT NULL, " +
                    "tape_index INTEGER NOT NULL, " +
                    "is_deleted BOOLEAN NOT NULL, " +
                    "PRIMARY KEY (id)" +
                "); ";
            ExecNonQuery(sql);
        }

        private NpgsqlConnection GetDBCon()
        {
            var data = Thread.GetData(Thread.GetNamedDataSlot("con"));
            if (data is null)
            {
                Log.Add(LogLevel.DEBUG, $"New DB connection created for this thread.");
                var con = new NpgsqlConnection($"Host={_pgHost};Username={_pgUser};Password={_pgPass};Database=tlfs;");
                Thread.SetData(Thread.GetNamedDataSlot("con"), con);
                con.Open();
                con.Close();
                return con;
            }
            else
            {
                return (NpgsqlConnection)data;
            }
        }

        public string GetVersion()
        {
            var con = GetDBCon();
            var sql = "SELECT version()";
            using var cmd = new NpgsqlCommand(sql, con);
            con.Open();
            var version = cmd.ExecuteScalar().ToString();
            con.Close();
            return $"PostgreSQL version: {version}";
        }

        private void ExecNonQuery(string sql)
        {
            var con = GetDBCon();
            using var cmd = new NpgsqlCommand(sql, con);
            con.Open();
            cmd.ExecuteNonQuery();
            con.Close();
        }

        private void ExecNonQuery(string sql, List<NpgsqlParameter> parameters)
        {
            var con = GetDBCon();
            using var cmd = new NpgsqlCommand(sql, con);
            cmd.Parameters.AddRange(parameters.ToArray());
            con.Open();
            cmd.ExecuteNonQuery();
            con.Close();
        }

        private object ExecScalar(string sql, List<NpgsqlParameter> parameters)
        {
            var con = GetDBCon();
            using var cmd = new NpgsqlCommand(sql, con);
            con.Open();
            cmd.Parameters.AddRange(parameters.ToArray());
            var scalar = cmd.ExecuteScalar();
            con.Close();
            return scalar;
        }

        private object ExecScalar(string sql)
        {
            var con = GetDBCon();
            using var cmd = new NpgsqlCommand(sql, con);
            con.Open();
            var scalar = cmd.ExecuteScalar();
            con.Close();
            return scalar;
        }

        public void SaveNewEntry(string path, int mode, long parentId, bool isDirectory)
        {
            var sql = "INSERT INTO file_entry (path, mode, is_directory, is_deleted, parent_id, accessed, modified, gid, uid, size, write_complete, file_hash) " +
            "VALUES (@path, @mode, @is_directory, @is_deleted, @parent_id, @accessed, @modified, @gid, @uid, @size, @write_complete, @file_hash);";
            int gid = 0;
            int uid = 0;
            long size = 0;
            var parameters = new List<NpgsqlParameter>()
            {
                new NpgsqlParameter("path", path),
                new NpgsqlParameter("mode", mode),
                new NpgsqlParameter("is_directory", isDirectory),
                new NpgsqlParameter("is_deleted", false),
                new NpgsqlParameter("parent_id", parentId),
                new NpgsqlParameter("accessed", DateTime.UtcNow),
                new NpgsqlParameter("modified", DateTime.UtcNow),
                new NpgsqlParameter("gid", gid),
                new NpgsqlParameter("uid", uid),
                new NpgsqlParameter("size", size),
                new NpgsqlParameter("file_hash", "")
            };
            if (isDirectory)
            {
                parameters.Add(new NpgsqlParameter("write_complete", true));
            }
            else
            {
                parameters.Add(new NpgsqlParameter("write_complete", false));
            }
            ExecNonQuery(sql, parameters);
        }

        public void UpdateEntryWriteComplete(long fileId, long size)
        {
            Log.Add(LogLevel.DEBUG, $"Updating file id {fileId} as write complete in database.");
            var sql = "UPDATE file_entry SET size = @size, write_complete = true WHERE id = @id;";
            var parameters = new List<NpgsqlParameter>()
            {
                new NpgsqlParameter("id", fileId),
                new NpgsqlParameter("size", size)
            };
            ExecNonQuery(sql, parameters);
        }

        public Entry GetEntry(string path)
        {
            var con = GetDBCon();
            var sql = "SELECT * FROM file_entry WHERE path = @path AND is_deleted = false;";
            Entry entry = null;
            using var cmd = new NpgsqlCommand(sql, con);
            cmd.Parameters.AddWithValue("path", path);
            con.Open();
            using var reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                entry = new Entry((int)reader["mode"], (bool)reader["is_directory"], (long)reader["id"],
                    (int)reader["uid"], (int)reader["gid"], (DateTime)reader["accessed"], (DateTime)reader["modified"],
                    (long)reader["parent_id"], (long)reader["size"]);
            }
            con.Close();
            return entry;
        }

        public List<string> GetDirectoryContents(long parentId)
        {
            var con = GetDBCon();
            var sql = "SELECT path FROM file_entry WHERE parent_id = @parent_id AND is_deleted = false;";
            var entryNames = new List<string>();
            using var cmd = new NpgsqlCommand(sql, con);
            cmd.Parameters.AddWithValue("parent_id", parentId);
            con.Open();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var path = reader["path"].ToString();
                entryNames.Add(path.Split('/').Last());
            }
            con.Close();
            return entryNames;
        }

        ///Returns a list of all tapes recorded in the database
        public List<Tape> GetTapes()
        {
            var con = GetDBCon();
            var sql = "SELECT * FROM tape;";
            var tapes = new List<Tape>();
            using var cmd = new NpgsqlCommand(sql, con);
            con.Open();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                tapes.Add(new Tape((int)reader["id"],
                                reader["barcode"].ToString(),
                                (long)reader["capacity"],
                                (bool)reader["is_full"],
                                (bool)reader["marked_for_removal"],
                                (int)reader["write_errors"]));
                
            }
            con.Close();
            return tapes;
        }

        public void AddTape(string barcode)
        {
            var sql = "INSERT INTO tape (barcode, capacity, is_full, marked_for_removal, write_errors) " +
            "VALUES (@barcode, @capacity, @is_full, @marked_for_removal, @write_errors);";
            long capacity = 0;
            if (barcode.EndsWith("L8"))
            {
                capacity = 12000000000000;
            }
            else if (barcode.EndsWith("M8"))
            {
                capacity = 900000000000;
            }
            int writeErrors = 0; 
            var parameters = new List<NpgsqlParameter>()
            {
                new NpgsqlParameter("barcode", barcode),
                new NpgsqlParameter("capacity", capacity),
                new NpgsqlParameter("is_full", false),
                new NpgsqlParameter("is_deleted", false),
                new NpgsqlParameter("marked_for_removal", false),
                new NpgsqlParameter("write_errors", writeErrors)
            };
            ExecNonQuery(sql, parameters);
        }

        public Tape GetTape(string barcode)
        {
            var con = GetDBCon();
            var sql = "SELECT * FROM tape WHERE barcode = @barcode;";
            Tape tape = null;
            using var cmd = new NpgsqlCommand(sql, con);
            cmd.Parameters.AddWithValue("barcode", barcode);
            con.Open();
            using var reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                tape = new Tape((int)reader["id"],
                            reader["barcode"].ToString(),
                            (long)reader["capacity"],
                            (bool)reader["is_full"],
                            (bool)reader["marked_for_removal"],
                            (int)reader["write_errors"]);
            }
            con.Close();
            return tape;
        }

        public Tape GetTape(int tapeId)
        {
            var con = GetDBCon();
            var sql = "SELECT * FROM tape WHERE id = @id;";
            Tape tape = null;
            using var cmd = new NpgsqlCommand(sql, con);
            cmd.Parameters.AddWithValue("id", tapeId);
            con.Open();
            using var reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                tape = new Tape((int)reader["id"],
                            reader["barcode"].ToString(),
                            (long)reader["capacity"],
                            (bool)reader["is_full"],
                            (bool)reader["marked_for_removal"],
                            (int)reader["write_errors"]);
            }
            con.Close();
            return tape;
        }

        ///Returns the total number of bytes consumed by trapped deleted file parts on this tape
        public long GetDeletedByteCountOnTape(string barcode)
        {
            var con = GetDBCon();
            var sql = "SELECT file_part_on_tape.blocks_written, file_part_on_tape.block_size " +
	                  "FROM file_part_on_tape, tape WHERE file_part_on_tape.is_deleted = true AND file_part_on_tape.tape_id = tape.id AND tape.barcode = @barcode;";
            long byteCount = 0;
            using var cmd = new NpgsqlCommand(sql, con);
            cmd.Parameters.AddWithValue("barcode", barcode);
            con.Open();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                byteCount += (long)reader["blocks_written"] * (int)reader["block_size"];
            }
            con.Close();
            return byteCount;
        }

        ///Returns false if there are no file parts on this tape
        public bool GetLastFilePartOnTape(string barcode, out int lastPartTapeIndex, out long lastPartId)
        {
            lastPartTapeIndex = -1;
            lastPartId = -1;
            var tapeHasBlock = false;
            var con = GetDBCon();
            var sql = "SELECT file_part_on_tape.id, file_part_on_tape.tape_index " +
	                  "FROM file_part_on_tape, tape WHERE tape.id = file_part_on_tape.tape_id AND tape.barcode = @barcode ORDER BY tape_index DESC LIMIT 1;";
            using var cmd = new NpgsqlCommand(sql, con);
            cmd.Parameters.AddWithValue("barcode", barcode);
            con.Open();
            using var reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                lastPartTapeIndex = (int)reader["tape_index"];
                lastPartId = (long)reader["id"];
                tapeHasBlock = true;
            }
            con.Close();
            return tapeHasBlock;
        }

        public long SaveNewFilePartOnTape(long entryId, string barcode, int tapeIndex, int filePartNumber)
        {
            var tape = GetTape(barcode);
            var sql = "INSERT INTO file_part_on_tape(tape_id, file_id, file_part_number, blocks_written, block_size, tape_index, is_deleted) " +
	                  "VALUES (@tape_id, @file_id, @file_part_number, 0, 0, @tape_index, false) " + 
                      "RETURNING id;";
            var parameters = new List<NpgsqlParameter>()
            {
                new NpgsqlParameter("file_id", entryId),
                new NpgsqlParameter("tape_id", tape.Id),
                new NpgsqlParameter("file_part_number", filePartNumber),
                new NpgsqlParameter("tape_index", tapeIndex)
            };
            return (long)ExecScalar(sql, parameters);
        }

        ///working in reverse order, deletes the file part on tape if it's the last part on the tape
        ///and is also marked as deleted
        ///todo: also deletes the file entry if it has no more associated file parts
        ///Marks the tape as no longer full if at least one part was deleted
        public void ClearDeletedFilePartsFromEndOfTape(int tapeId)
        {
            var filePartsToDelete = new List<long>();
            var con = GetDBCon();
            var sql = "SELECT file_part_on_tape.id, file_part_on_tape.is_deleted FROM file_part_on_tape " + 
                      "WHERE file_part_on_tape.tape_id = @tape_id " + 
                      "ORDER BY file_part_on_tape.tape_index DESC;";
            using var cmd = new NpgsqlCommand(sql, con);
            cmd.Parameters.AddWithValue("tape_id", tapeId);
            con.Open();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                if ((bool)reader["is_deleted"])
                {
                    filePartsToDelete.Add((long)reader["id"]);
                }
                else
                {
                    break;
                }
            }
            con.Close();
            if (filePartsToDelete.Count > 0)
            {
                sql = "DELETE FROM file_part_on_tape WHERE file_part_on_tape.is_deleted = true AND id = @id";
                foreach(long id in filePartsToDelete)
                {
                    var filePartParameters = new List<NpgsqlParameter>()
                    {
                        new NpgsqlParameter("id", id)
                    };
                    ExecNonQuery(sql, filePartParameters);
                }
                sql = "UPDATE tape SET is_full = false WHERE id = @tape_id;";
                var tapeParameters = new List<NpgsqlParameter>()
                {
                    new NpgsqlParameter("tape_id", tapeId)
                };
                ExecNonQuery(sql, tapeParameters);
            }
        }

        public void ClearDeletedFilePartsFromEndOfTape(string barcode)
        {
            var sql = "SELECT id FROM tape WHERE barcode = @barcode LIMIT 1;";
            var parameters = new List<NpgsqlParameter>()
            {
                new NpgsqlParameter("barcode", barcode)
            };
            var tapeId = (int)ExecScalar(sql, parameters);
            ClearDeletedFilePartsFromEndOfTape(tapeId);
        }

        public string GetTapeBarcode(int tapeId)
        {
            var sql = "SELECT barcode FROM tape WHERE id = @id;";
            var parameters = new List<NpgsqlParameter>()
            {
                new NpgsqlParameter("id", tapeId)
            };
            return (string)ExecScalar(sql, parameters);
        }

        ///Incomplete files and file parts can be caused by improper shudown of the filesystem
        public void DeleteIncompleteFilesAndFilePartsOnTape()
        {
            Log.Add(LogLevel.DEBUG, "Deleting incomplete files and file parts on tape.");
            //Mark incomplete file writes as deleted - perhaps the filesystem was killed during a write
            var sql = "UPDATE file_entry SET is_deleted = true WHERE write_complete = false";
            ExecNonQuery(sql);
            //Mark the file parts on tape of incomplete file writes as deleted - perhaps the filesystem was killed during a write
            sql = "UPDATE file_part_on_tape SET is_deleted = true FROM file_entry WHERE file_part_on_tape.file_id = file_entry.id AND file_entry.write_complete = false;";
            ExecNonQuery(sql);
            //Mark the file parts on tape of deleted files as deleted - perhaps the file system was killed during a delete
            sql = "UPDATE file_part_on_tape SET is_deleted = true FROM file_entry WHERE file_part_on_tape.file_id = file_entry.id AND file_entry.is_deleted = true;";
            ExecNonQuery(sql);
            //Remove deleted files that have no more file parts on tape
            sql = "DELETE FROM file_entry WHERE file_entry.is_deleted = true AND (SELECT COUNT(*) FROM file_part_on_tape WHERE file_part_on_tape.file_id = file_entry.id) = 0;";
            ExecNonQuery(sql);
        }

        public void ClearDeletedEntry(long entryId)
        {
            var sql = "DELETE FROM file_entry WHERE id = @id AND is_deleted = true;";
            var parameters = new List<NpgsqlParameter>()
            {
                new NpgsqlParameter("id", entryId)
            };
            ExecNonQuery(sql, parameters);
        }

        public void MarkFilePartOnTapeAsDeleted(long filePartOnTapeId)
        {
            var sql = "UPDATE file_part_on_tape SET is_deleted = true WHERE id = @id;";
            var parameters = new List<NpgsqlParameter>()
            {
                new NpgsqlParameter("id", filePartOnTapeId)
            };
            ExecNonQuery(sql, parameters);
        }

        public void MarkEntryAsDeleted(long entryId)
        {
            var sql = "UPDATE file_entry SET is_deleted = true WHERE id = @id";
            var parameters = new List<NpgsqlParameter>()
            {
                new NpgsqlParameter("id", entryId)
            };
            ExecNonQuery(sql, parameters);
        }

        ///bypass the usual mark as deleted step in cases where the file part on tape was not actually written
        public void DeleteFilePartOnTape(long filePartOnTapeId)
        {
            var sql = "DELETE FROM file_part_on_tape WHERE id = @id;";
            var parameters = new List<NpgsqlParameter>()
            {
                new NpgsqlParameter("id", filePartOnTapeId)
            };
            ExecNonQuery(sql, parameters);
        }


        public void DeleteEntry(long filePartOnTapeId)
        {
            var sql = "UPDATE file_entry SET is_deleted = true WHERE id = @id;";
            var parameters = new List<NpgsqlParameter>()
            {
                new NpgsqlParameter("id", filePartOnTapeId)
            };
            ExecNonQuery(sql, parameters);
        }

        //Get the file parts on tape, ordered by file part number
        public Dictionary<int, FilePartOnTape> GetFilePartsOnTape(string path)
        {
            var con = GetDBCon();
            var sql = "SELECT file_part_on_tape.id, file_part_on_tape.tape_id, file_part_on_tape.file_id, " + 
                      "file_part_on_tape.file_part_number, file_part_on_tape.blocks_written, file_part_on_tape.block_size, " + 
                      "file_part_on_tape.tape_index " +
	                  "FROM file_part_on_tape, file_entry " + 
                      "WHERE file_part_on_tape.file_id = file_entry.id AND file_entry.path = @path AND file_part_on_tape.is_deleted = false " +
                      "ORDER BY file_part_on_tape.file_part_number;";
            var filePartsOnTape = new Dictionary<int, FilePartOnTape>();
            using var cmd = new NpgsqlCommand(sql, con);
            cmd.Parameters.AddWithValue("path", path);
            con.Open();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                filePartsOnTape.Add((int)reader["file_part_number"], new FilePartOnTape(
                    (long)reader["id"],
                    (int)reader["tape_id"],
                    (long)reader["file_id"],
                    (long)reader["blocks_written"],
                    (int)reader["block_size"],
                    (int)reader["tape_index"]
                ));
            }
            con.Close();
            return filePartsOnTape;
        }

        ///Mark the tape as full if a write attempt has failed due to no space left on device
        public void MarkTapeAsFull(string barcode)
        {
            var sql = "UPDATE tape SET is_full = true WHERE barcode = @barcode;";
            var parameters = new List<NpgsqlParameter>()
            {
                new NpgsqlParameter("barcode", barcode)
            };
            ExecNonQuery(sql, parameters);
        }

        ///Estimates the total capacity of all tapes in the library
        public decimal GetTotalEstimatedTapeCapacity()
        {
            var sql = "SELECT sum(capacity) as capacity FROM tape WHERE marked_for_removal = false;";
            var response = ExecScalar(sql);
            if (response == DBNull.Value)
            {
                return 0;
            }
            else
            {
                return (decimal)response;
            }
        }

        ///Return the total bytes consumed by all files on tape including partial writes and deleted files
        public decimal GetTotalConsumedSpace()
        {
            var sql = "SELECT sum(size) as size FROM file_entry;";
            var response = ExecScalar(sql);
            if (response == DBNull.Value)
            {
                return 0;
            }
            else
            {
                return (decimal)response;
            }
        }

        ///Return the total bytes consumed by all files on tape excluding partial writes and deleted files
        public decimal GetTotalUsefulConsumedSpace()
        {
            var sql = "SELECT sum(size) as size FROM file_entry WHERE write_complete = true AND is_deleted = false;";
            var response = ExecScalar(sql);
            if (response == DBNull.Value)
            {
                return 0;
            }
            else
            {
                return (decimal)response;
            }
        }

        ///Returns the highest file entry ID, used to calculate remaining entry IDs
        public decimal GetMaxFileId()
        {
            var sql = "SELECT id FROM file_entry ORDER BY id DESC LIMIT 1;";
            var response = ExecScalar(sql);
            if (response == null)
            {
                return 0;
            }
            else
            {
                return (long)response;
            }
        }

        ///Updates the path of the entry in the database
        public void RenameEntry(long entryId, string newPath)
        {
             var sql = "UPDATE file_entry SET path = @new_path WHERE id = @id;";
            var parameters = new List<NpgsqlParameter>()
            {
                new NpgsqlParameter("new_path", newPath),
                new NpgsqlParameter("id", entryId)
            };
            ExecNonQuery(sql, parameters);
        }

        public void UpdateEntryOwner(long entryId, int uid, int gid)
        {
            var sql = "UPDATE file_entry SET gid = @gid, uid = @uid " +
            "WHERE id = @id;";
            var parameters = new List<NpgsqlParameter>()
            {
                new NpgsqlParameter("gid", gid),
                new NpgsqlParameter("uid", uid),
                new NpgsqlParameter("id", entryId)
            };
            ExecNonQuery(sql, parameters);
        }

        public void UpdateTimeStamps(long entryId, DateTime accessed, DateTime modified)
        {
            var sql = "UPDATE file_entry SET accessed = @accessed, modified = @modified " +
            "WHERE id = @id;";
            var parameters = new List<NpgsqlParameter>()
            {
                new NpgsqlParameter("accessed", accessed),
                new NpgsqlParameter("modified", modified),
                new NpgsqlParameter("id", entryId)
            };
            ExecNonQuery(sql, parameters);
        }

        public void UpdateEntryMode(long entryId, int mode)
        {
            var sql = "UPDATE file_entry SET mode = @mode " +
            "WHERE id = @id;";
            var parameters = new List<NpgsqlParameter>()
            {
                new NpgsqlParameter("mode", mode),
                new NpgsqlParameter("id", entryId)
            };
            ExecNonQuery(sql, parameters);
        }
    }
}