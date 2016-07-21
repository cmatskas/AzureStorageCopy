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

        private CloudStorageAccount sourceStorageAccount;
        private CloudStorageAccount destinationStorageAccount;
        private CloudBlobClient blobClient;
        private CloudQueueClient queueClient;
        private static ILogger logger;

        public AzureUtil()
        {
            InitialiseLogger();
        }
        

        public CloudBlob GetCloudBlob(StorageLocation storageLocation, string containerName, string blobName, BlobType blobType)
        {
            var client = GetCloudBlobClient(storageLocation);
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

        public string GetContainerFromBlobUri(string blobUri)
        {
            return blobUri.Split('/').Reverse().Skip(1).Take(1).FirstOrDefault();
        }

        public List<string> GetAllBlobReferencesInContainer(string containerName, StorageLocation storageLocation)
        {
            var client = GetCloudBlobClient(storageLocation);
            var container = client.GetContainerReference(containerName);
            var result = container.ListBlobs(useFlatBlobListing: true, blobListingDetails: BlobListingDetails.None);
            return result.Select(b => b.Uri.ToString()).ToList();
        }

        public List<string> GetAllBlobsInStorageAccount(StorageLocation storageLocation)
        {
            var allBlobs = new HashSet<string>();

            var client = GetCloudBlobClient(storageLocation);
            var containers = client.ListContainers();
            foreach (var container in containers)
            {
                var blobs = container.ListBlobs(useFlatBlobListing: true, blobListingDetails: BlobListingDetails.None);
                allBlobs.UnionWith(blobs.Select(b => b.Uri.ToString()));
            }

            return allBlobs.ToList();
        }

        public CloudQueue GetCloudQueue(string queueName)
        {
            var client = GetCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            queue.CreateIfNotExists();
            return queue;
        }

        public CloudBlobContainer GetCloudBlobContainer(string containerName, StorageLocation storageLocation)
        {
            var client = GetCloudBlobClient(storageLocation);
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
            var destinationBlob = GetCloudBlob(StorageLocation.destination, destinationContainer, destinationBlobName, BlobType.BlockBlob);

            // copy only if the destination resource doesnt' exits
            // uncomment if this is causing issues with copying the files accross

            CopyOptions copyOptions = null;
            // copyOptions = new CopyOptions();
            // copyOptions.DestinationAccessCondition = AccessCondition.GenerateIfNotExistsCondition();

            try
            {
                await TransferManager.CopyAsync(new Uri(sourceBlobUri), destinationBlob, true, copyOptions, null);
                logger.Information("Blob copied successfully");
            }
            catch (Exception e)
            {
                logger.Error("Exception during copy operation {@Exception}", e);
            }

            logger.Information("CloudBlob {@SourceBlobUri} is copied to {@destinationBlob} successfully.", sourceBlobUri, destinationBlob.Uri.ToString());
        }

        private CloudBlobClient GetCloudBlobClient(StorageLocation storageLocation)
        {
            blobClient = blobClient ?? GetStorageAccount(storageLocation).CreateCloudBlobClient();
            return blobClient;
        }

        private CloudQueueClient GetCloudQueueClient()
        {
            queueClient = queueClient ?? GetStorageAccount().CreateCloudQueueClient();
            return queueClient;
        }

        private string LoadConnectionStringFromConfigration(StorageLocation storageLocation = StorageLocation.source)
        {
            var storageConnectionString = storageLocation == StorageLocation.source
                    ? "SourceStorageConnectionString"
                    : "DestinationStorageConnectionString";


            return CloudConfigurationManager.GetSetting(storageConnectionString);
        }

        private CloudStorageAccount GetStorageAccount(StorageLocation storageLocation = StorageLocation.source)
        {
            if (storageLocation == StorageLocation.source)
            {
                if (sourceStorageAccount == null)
                {
                    var sourceConnectionString = LoadConnectionStringFromConfigration();
                    sourceStorageAccount = CloudStorageAccount.Parse(sourceConnectionString);
                }

                return sourceStorageAccount;
            }

            if (destinationStorageAccount == null)
            {
                var destinationConnectionString = LoadConnectionStringFromConfigration(StorageLocation.destination);
                destinationStorageAccount = CloudStorageAccount.Parse(destinationConnectionString);
            }

            return destinationStorageAccount;
        }

        private void InitialiseLogger()
        {
            var storage = GetStorageAccount(StorageLocation.source);

            logger = new LoggerConfiguration()
                             .WriteTo.AzureTableStorage(storage)
                             .MinimumLevel.Debug()
                             .CreateLogger();
        }

    }
}
