using System;
using System.Collections.Generic;
using System.Text;
using Azure.Storage.Blobs.Models;

namespace SqlCollaborative.Azure.DataPipelineTools.Common
{
    public static class StringEx
    {
        /// <summary>
        /// Indicates whether the specified string is not null or an empty string ("").
        /// </summary>
        /// <param name="str">The string to test.</param>
        /// <returns>false if the value parameter is null or an empty string (""); otherwise, true.</returns>
        public static bool IsNotNullOrEmpty(string str)
        {
            return !string.IsNullOrEmpty(str);
        }

        /// <summary>
        /// Indicates whether a specified string is not null, empty, or consists only of white-space characters.
        /// </summary>
        /// <param name="str">The string to test.</param>
        /// <returns>false if the value parameter is null or Empty, or if value consists exclusively of white-space characters; otherwise, true.</returns>
        public static bool IsNotIsNullOrWhiteSpace(string str)
        {
            return !string.IsNullOrWhiteSpace(str);
        }
    }
}
