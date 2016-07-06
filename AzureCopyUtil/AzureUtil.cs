using System;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.File;

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

        public static ICloudBlob GetCloubBlob(string blobUri)
        {
            var client = GetCloudBlobClient();
            return client.GetBlobReferenceFromServer(new Uri(blobUri));
        }

        public static CloudBlobDirectory GetCloudBlobDirectory(string containerName, string directoryName)
        {
            CloudBlobClient client = GetCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference(containerName);
            container.CreateIfNotExists();

            return container.GetDirectoryReference(directoryName);
        }

        public static string GetBlobNameFromUri(string blobUri)
        {
            return blobUri.Split(@'/').Last();
        }

        private static CloudBlobClient GetCloudBlobClient()
        {
            return blobClient ?? GetStorageAccount().CreateCloudBlobClient();
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
