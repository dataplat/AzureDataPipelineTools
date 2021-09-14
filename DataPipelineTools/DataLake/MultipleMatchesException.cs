using System;
using System.Collections.Generic;
using System.Text;

namespace SqlCollaborative.Azure.DataPipelineTools.DataLake
{
    public class MultipleMatchesException: Exception
    {
        public MultipleMatchesException(string message) : base(message) { }

        public MultipleMatchesException(string message, Exception innerException) : base(message, innerException) { }
    }
}
