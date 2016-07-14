using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage.Queue;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.DataMovement;
using Serilog;

namespace AzureCopyUtil
{
    /// <summary>
    /// A helper class provides convenient operations against storage account configured in the App.config.
    /// </summary>
    public class AzureUtil
    {

        private CloudStorageAccount storageAccount;
        private CloudBlobClient blobClient;
        private CloudQueueClient queueClient;
        private ILogger logger;

        public AzureUtil()
        {
            logger = Log.ForContext<AzureUtil>();
        }
        

        public CloudBlob GetCloudBlob(string containerName, string blobName, BlobType blobType)
        {
            var client = GetCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference(containerName);
            container.CreateIfNotExists();

            CloudBlob cloudBlob;
            switch (blobType)
            {
                case BlobType.AppendBlob:
                    cloudBlob = container.GetAppendBlobReference(blobName);
                    break;
                case BlobType.BlockBlob:
                    cloudBlob = container.GetBlockBlobReference(blobName);
                    break;
                case BlobType.PageBlob:
                    cloudBlob = container.GetPageBlobReference(blobName);
                    break;
                case BlobType.Unspecified:
                default:
                    throw new ArgumentException(string.Format("Invalid blob type {0}", blobType.ToString()), "blobType");
            }

            return cloudBlob;
        }

        public List<string> GetAllBlobReferencesInContainer(string containerName)
        {
            var client = GetCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var result = container.ListBlobs(useFlatBlobListing: true, blobListingDetails: BlobListingDetails.None);
            return result.Select(b => b.Uri.ToString()).ToList();
        }

        public CloudQueue GetCloudQueue(string queueName)
        {
            var client = GetCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            queue.CreateIfNotExists();
            return queue;
        }

        public ICloudBlob GetCloubBlob(string blobUri)
        {
            var client = GetCloudBlobClient();
            return client.GetBlobReferenceFromServer(new Uri(blobUri));
        }

        public CloudBlobContainer GetCloudBlobContainer(string containerName)
        {
            var client = GetCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            container.CreateIfNotExists();

            return container;
        }

        public string GetBlobNameFromUri(string blobUri)
        {
            return blobUri.Split('/').Last();
        }

        public CloudQueueMessage GetQueueItem(string queueName)
        {
            var queue = GetCloudQueue(queueName);
            return queue.GetMessage();
        }

        public void DeleteQueueMessage(string queueName, CloudQueueMessage queueMessageToDelete)
        {
            var queue = GetCloudQueue(queueName);
            queue.DeleteMessage(queueMessageToDelete);
            logger.Information("Queue message deleted");
        }

        public async Task CopyBlob(string sourceBlobUri, string destinationContainer)
        {
            var destinationBlobName = GetBlobNameFromUri(sourceBlobUri);
            var destinationBlob = GetCloudBlob(destinationContainer, destinationBlobName, BlobType.BlockBlob);

            try
            {
                await TransferManager.CopyAsync(new Uri(sourceBlobUri), destinationBlob, true);
                logger.Information("Blob copied successfully");
            }
            catch (Exception e)
            {
                logger.Error("Exception during copy operation {@Exception}", e);
            }

            logger.Information("CloudBlob {@SourceBlobUri} is copied to {@destinationBlob} successfully.", sourceBlobUri, destinationBlob.Uri.ToString());
        }

        private CloudBlobClient GetCloudBlobClient()
        {
            blobClient = blobClient ?? GetStorageAccount().CreateCloudBlobClient();
            return blobClient;
        }

        private CloudQueueClient GetCloudQueueClient()
        {
            queueClient = queueClient ?? GetStorageAccount().CreateCloudQueueClient();
            return queueClient;
        }

        private string LoadConnectionStringFromConfigration()
        {
            return CloudConfigurationManager.GetSetting("StorageConnectionString");
        }

        private CloudStorageAccount GetStorageAccount()
        {
            if (storageAccount == null)
            {
                var connectionString = LoadConnectionStringFromConfigration();
                storageAccount = CloudStorageAccount.Parse(connectionString);
            }

            return storageAccount;
        }

    }
}
