using System.Threading.Tasks;
using AzureCopyUtil;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;

namespace AzStorageCopy
{
    class Program
    {
        static void Main(string[] args)
        {
            var parallelTaskCount = args.Length == 0 ? 1000 : int.Parse(args[0]);

            Parallel.For(0, parallelTaskCount, i => 
            {
                CopyBlob();
            });
        }

        public static void CopyBlob()
        {
            var queueName = "video-queue";
            var azureUtil = new AzureUtil();

            CloudQueueMessage nextQueueItem;
            while(true)
            {
                nextQueueItem = azureUtil.GetQueueItem(queueName);
                if(nextQueueItem == null)
                {
                    return;
                }

                var destinationContainer = azureUtil.GetContainerFromBlobUri(nextQueueItem.AsString);
                azureUtil.CopyBlob(nextQueueItem.AsString, destinationContainer).GetAwaiter().GetResult();
                azureUtil.DeleteQueueMessage(queueName, nextQueueItem);

            }
        }

    }
}
