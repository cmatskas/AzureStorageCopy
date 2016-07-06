using System;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.DataMovement;
using System.Threading.Tasks;
using AzureCopyUtil;

namespace AzStorageCopy
{
    class Program
    {
        static void Main(string[] args)
        {
            //TODO : Replace all console logging with LibLog
            var p = new Program();
            var blobUrlToCopy = p.GetQueueItem().GetAwaiter().GetResult();
            p.CopyBlob(blobUrlToCopy, "cm-test-container").GetAwaiter().GetResult();
        }

        public async Task<string> GetQueueItem()
        {
            //TODO: Add code to retrieve code from Azure Storage Queue
            return "hello world";
        }

        public async Task CopyBlob(string sourceBlobUri, string destinationContainer)
        {
            var destinationBlobName = AzureUtil.GetBlobNameFromUri(sourceBlobUri);
            var destinationBlob = AzureUtil.GetCloudBlob(destinationContainer, destinationBlobName, BlobType.BlockBlob);
                     
            //TODO: Add Timer for statistics

            try
            {
                await TransferManager.CopyAsync(new Uri(sourceBlobUri), destinationBlob, false);
            }
            catch (Exception e)
            {
                //Log error to separate file to ensure that uncopied files can be transferred at a later point
                Console.WriteLine(e.Message);
            }
        
            Console.WriteLine("CloudBlob {0} is copied to {1} successfully.", sourceBlobUri, destinationBlob.Uri.ToString());
        }
    }
}
