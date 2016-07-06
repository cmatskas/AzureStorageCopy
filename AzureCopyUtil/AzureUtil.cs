using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.File;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage.Queue;
using System.Collections.Generic;
using System.Linq;

namespace AzureCopyUtil
{
    /// <summary>
    /// A helper class provides convenient operations against storage account configured in the App.config.
    /// </summary>
    public class AzureUtil
    {
        private static CloudStorageAccount storageAccount;
        private static CloudBlobClient blobClient;
        private static CloudFileClient fileClient;
        private static CloudQueueClient queueClient;

        public static CloudBlob GetCloudBlob(string containerName, string blobName, BlobType blobType)
        {
            CloudBlobClient client = GetCloudBlobClient();
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

        public static List<string> GetAllBlobReferencesInContainer(string containerName)
        {
            CloudBlobClient client = GetCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference(containerName);
            var result = container.ListBlobs(useFlatBlobListing: true, blobListingDetails: BlobListingDetails.None) as List<CloudBlockBlob>;
            return result.Select(b => b.Uri.ToString()).ToList();
        }

        public static CloudQueue GetCloudQueue(string queueName)
        {
            CloudQueueClient client = GetCloudQueueClient();
            CloudQueue queue = client.GetQueueReference(queueName);
            queue.CreateIfNotExists();
            return queue;
        }

        public static ICloudBlob GetCloubBlob(string blobUri)
        {
            var client = GetCloudBlobClient();
            return client.GetBlobReferenceFromServer(new Uri(blobUri));
        }

        public static CloudBlobContainer GetCloudBlobContainer(string containerName)
        {
            CloudBlobClient client = GetCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference(containerName);
            container.CreateIfNotExists();

            return container;
        }

        public static string GetBlobNameFromUri(string blobUri)
        {
            return blobUri.Split(@'/').Last();
        }

        private static CloudBlobClient GetCloudBlobClient()
        {
            return blobClient ?? GetStorageAccount().CreateCloudBlobClient();
        }

        private static CloudQueueClient GetCloudQueueClient()
        {
            return queueClient ?? GetStorageAccount().CreateCloudQueueClient();
        }

        private static string LoadConnectionStringFromConfigration()
        {
            return CloudConfigurationManager.GetSetting("StorageConnectionString");
        }

        private static CloudStorageAccount GetStorageAccount()
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
