using System.Diagnostics;
using System;

namespace Tlfs
{
    static class ProcessHelper
    {
        public static string ExecProcess(string path, string arguments)
        {
            string error;
            string output;
            int exitCode;
            using (var process = new Process())
            {
                process.StartInfo.UseShellExecute = false;
                process.StartInfo.CreateNoWindow = true;
                process.StartInfo.FileName = path;
                process.StartInfo.Arguments = arguments;
                process.StartInfo.RedirectStandardOutput = true;
                process.StartInfo.RedirectStandardError = true;
                error = null;
                process.ErrorDataReceived += new DataReceivedEventHandler((sender, e) =>
                    { error += e.Data; });
                process.Start();
                process.BeginErrorReadLine();
                output = process.StandardOutput.ReadToEnd();  
                process.WaitForExit();
                exitCode = process.ExitCode;
            }
            if (exitCode != 0)
            {
                throw new Exception($"Process error message: {error} Exit code: {exitCode}");
            }
            return output;
        }
    }
}