using System;
using System.Collections.Generic;
using System.Text;
using DataPipelineTools.Tests.Common;

namespace DataPipelineTools.Functions.Tests
{
    /// <summary>
    /// Base class for functions tests. Exposes the 
    /// </summary>
    public abstract class IntegrationTestBase : TestBase
    {
        protected bool IsRunningOnCIServer
        {
            get
            {
                // Github Actions
                if (Environment.GetEnvironmentVariable("CI") != null)
                    return true;

                //Azure Devops
                if (Environment.GetEnvironmentVariable("TF_BUILD") != null)
                    return true;

                return false;
            }
        }

        protected void StartLocalFunctionsInstance()
        {
            throw new NotImplementedException();
        }

    }
}
