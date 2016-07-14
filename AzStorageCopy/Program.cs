using System.Threading.Tasks;
using AzureCopyUtil;
using Microsoft.WindowsAzure.Storage.Queue;

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
