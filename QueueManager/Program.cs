using Microsoft.WindowsAzure.Storage.Queue;
using AzureCopyUtil;
using System.IO;
using System;

namespace QueueManager
{
    public class Program
    {
        static void Main(string[] args)
        {
            AddAllBlobsToQueue("video-queue");
            //UploadBlobs("videos", @"C:\Users\chmatsk\Videos\VS2015 - IntelliTrace\IntelliTraceInVisualStudio2015DemoOnly_high.mp4");
        }

        private static void AddAllBlobsToQueue(string queueName)
        {
            var azureUtil = new AzureUtil();
            var queue = azureUtil.GetCloudQueue(queueName);
            var blobUris = azureUtil.GetAllBlobsInStorageAccount();
            
            foreach(var blobUri in blobUris)
            {
                queue.AddMessage(new CloudQueueMessage(blobUri));
                Console.WriteLine($"Added {blobUri} to the queue");
            }
        }

        private static void UploadBlobs(string containerName, string filePath)
        {
            const string blobName = "testBlob_";
            var azureUtil = new AzureUtil();
            var container = azureUtil.GetCloudBlobContainer(containerName);
            var counter = 0;
            while (counter < 1000)
            {
                var blob = container.GetBlockBlobReference(blobName + counter);
                using (var fileStream = File.OpenRead(filePath))
                {
                    blob.UploadFromStream(fileStream);
                }

                Console.WriteLine($"Uploaded {counter} out of 1000 videos");
                counter++;
            } 
        }
    }
}
