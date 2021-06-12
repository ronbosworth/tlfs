using System;
using System.IO;
using System.Collections.Generic;

namespace Tlfs
{
    class Config
    {
        public string Mountpoint;
        public string PgHost;
        public string PgUser;
        public string PgPass;
        public string ConfigLocation;
        public List<string> DrivePaths;
        public string ChangerPath;

        public Config(string location)
        {
            DrivePaths = new List<string>();
            ConfigLocation = location;
            string[] lines;
            if (File.Exists(location))
            {
                lines = File.ReadAllLines(location);
                foreach(string line in lines)
                {
                    try
                    {
                        if (!line.Contains('=')) { continue; }
                        var kvpair = SplitAtFirstChar(line, '=');
                        var key = kvpair[0].Trim().ToLower();
                        var value = kvpair[1].Trim();
                        switch(key)
                        {
                            case "mountpoint":
                                Mountpoint = RemoveTrailingSlash(value);
                                break;
                            case "pghost":
                                PgHost = value;
                                break;
                            case "pguser":
                                PgUser = value;
                                break;
                            case "pgpass":
                                PgPass = value;
                                break;
                            case "drive path":
                                DrivePaths.Add(value);
                                break;
                            case "changer path":
                                ChangerPath = value;
                                break;
                            case "log level":
                                switch(value.ToLower())
                                {
                                    case "error":
                                        Log.CurrentLevel = LogLevel.ERROR;
                                        break;
                                    case "info":
                                        Log.CurrentLevel = LogLevel.INFO;
                                        break;
                                    default:
                                        Log.CurrentLevel = LogLevel.DEBUG;
                                        break;
                                }
                                break;
                            case "":
                                break;
                            default:
                                Log.Add(LogLevel.ERROR, $"Unknown config key found while parsing config: {key}");
                                break;
                        }
                    }
                    catch (Exception e)
                    {
                        Log.Add(LogLevel.ERROR, $"Parsing line {line} in config: {e.Message}");
                        Log.Add(LogLevel.ERROR, e.StackTrace);
                    }
                }
            }
            else
            {
                throw new Exception($"Could not find a config file at: {location}");
            }
        }

        private string[] SplitAtFirstChar(string text, char character)
        {
            int index = text.IndexOf(character);
            string[] split = {
                              text.Substring(0, index),
                              text.Substring(index + 1, text.Length - (index + 1))
            };
            return split;
        }

        private string RemoveTrailingSlash(string path)
        {
            if (path.EndsWith('/'))
            {
                path = path.Substring(0, path.Length - 1);
            }
            return path;
        }
    }
}