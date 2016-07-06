using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using AzureCopyUtil;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;

namespace QueueManager
{
    public class Program
    {
        static void Main(string[] args)
        {
            AddAllBlobsToQueue(args[0], args[1]);
        }

        private static void AddAllBlobsToQueue(string queueName, string containerName)
        {
            var queue = AzureUtil.GetCloudQueue(queueName);
            var blobUris = AzureUtil.GetAllBlobReferencesInContainer(containerName);
            
            foreach(var blobUri in blobUris)
            {
                queue.AddMessage(new CloudQueueMessage(blobUri));
            }
        }

        private static void UploadBlobs(string containerName, string filePath)
        {
            const string blobName = "testBlob_";
            var container = AzureUtil.GetCloudBlobContainer(containerName);
            var counter = 0;
            while (counter < 1000)
            {
                var blob = container.GetBlockBlobReference(blobName + counter);
                using (var fileStream = File.OpenRead(filePath))
                {
                    blob.UploadFromStream(fileStream);
                }
            } 

        }
    }
}
