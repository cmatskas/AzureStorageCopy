using System;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.DataMovement;
using System.Threading.Tasks;
using AzureCopyUtil;
using Microsoft.WindowsAzure.Storage;
using Serilog;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage.Queue;

namespace AzStorageCopy
{
    class Program
    {
        static void Main(string[] args)
        {
            InitialiseLogger();
            Parallel.For(0, 100, i => 
            {
                CopyBlob();
            });
        }

        public static void InitialiseLogger()
        {
            var storage = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));

            var log = new LoggerConfiguration()
                             .WriteTo.AzureTableStorage(storage)
                             .MinimumLevel.Debug()
                             .CreateLogger();
        }

        public static void CopyBlob()
        {
            var queueName = "video-queue";
            var destinationContainer = "cm-test-container";
            var azureUtil = new AzureUtil();

            CloudQueueMessage nextQueueItem;
            do
            {
                nextQueueItem = azureUtil.GetQueueItem(queueName);
                azureUtil.CopyBlob(nextQueueItem.AsString, destinationContainer).GetAwaiter().GetResult();
                azureUtil.DeleteQueueMessage(queueName, nextQueueItem);

            } while (nextQueueItem != null);
        }

    }
}
