using System;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;


namespace EventHubGenerator.Ratings
{
    public class RatingsFunction
    {
        private readonly ILogger _logger;

        public RatingsFunction(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<RatingsFunction>();
        }

        private const string PROGRESS_CONTAINER = "progress";
        private const string PROGRESS_BLOB = "ratings-progress.txt";
    
        [Function("RatingsGenerator")]
        public async Task Run([TimerTrigger("0 */5 * * * *")] TimerInfo myTimer)
        {
            var eventHubConnection = Environment.GetEnvironmentVariable("EVENTHUB_CONNECTION_STRING") 
                ?? throw new InvalidOperationException("EVENTHUB_CONNECTION_STRING not configured");
            
            var producerClient = new EventHubProducerClient(eventHubConnection);

            try
            {
                string storageConnectionString = Environment.GetEnvironmentVariable("STORAGE_CONTAINER_STRING") 
                    ?? throw new InvalidOperationException("STORAGE_CONTAINER_STRING not configured");
                string storageContainer = Environment.GetEnvironmentVariable("STORAGE_CONTAINER") 
                    ?? throw new InvalidOperationException("STORAGE_CONTAINER not configured");
                string filePath = Environment.GetEnvironmentVariable("RATINGS_FILEPATH") 
                    ?? throw new InvalidOperationException("RATINGS_FILEPATH not configured");
                
                int totalLines = await GetTotalLinesAsync(storageConnectionString, storageContainer, filePath);
                int lastProcessedLine = await GetLastProcessedLineAsync(storageConnectionString);
                _logger.LogInformation($"Last processed line: {lastProcessedLine}");

                int currentLine = 0;
                int processedInThisRun = 0;
                const int MAX_LINES_PER_RUN = 250;

                await foreach (var line in ReadBlobStorageFileAsync(storageConnectionString, storageContainer, filePath))
                {
                    if (currentLine++ < lastProcessedLine)
                        continue;

                    if (processedInThisRun >= MAX_LINES_PER_RUN)
                    {
                        _logger.LogInformation($"Already processed: {processedInThisRun}, current: {currentLine}");
                        break;
                    }

                    var userRating = ToUserRating(line);
                    if (userRating != null)
                    {
                        var json = JsonConvert.SerializeObject(userRating);
                        _logger.LogInformation(json);

                        using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
                        eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(json)));
                        await producerClient.SendAsync(eventBatch);

                        processedInThisRun++;
                        
                        if (currentLine >= totalLines)
                        {
                            await SaveProgressAsync(storageConnectionString, 1);
                            _logger.LogInformation("Reached end of file, resetting to beginning");
                        }
                        else
                        {
                            await SaveProgressAsync(storageConnectionString, currentLine);
                        }
                    }

                    await Task.Delay(1000);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"An error occurred: {ex}");
            }
            finally
            {
                await producerClient.DisposeAsync();
            }
        }

        private async Task<int> GetTotalLinesAsync(
            string connectionString, 
            string containerName, 
            string filePath)
        {
            try
            {
                var count = 0;
                await foreach (var _ in ReadBlobStorageFileAsync(connectionString, containerName, filePath))
                {
                    count++;
                }
                return count - 1;
            }
            catch
            {
                return 0;
            }
        }

        private async Task<int> GetLastProcessedLineAsync(string storageConnectionString)
        {
            var blobServiceClient = new BlobServiceClient(storageConnectionString);
            var containerClient = blobServiceClient.GetBlobContainerClient(PROGRESS_CONTAINER);
            await containerClient.CreateIfNotExistsAsync();
            
            var blobClient = containerClient.GetBlobClient(PROGRESS_BLOB);
            if (!await blobClient.ExistsAsync())
                return 1;

            var response = await blobClient.DownloadContentAsync();
            var content = Encoding.UTF8.GetString(response.Value.Content);
            return int.Parse(content);
        }

        private async Task SaveProgressAsync(string storageConnectionString, int lineNumber)
        {
            var blobServiceClient = new BlobServiceClient(storageConnectionString);
            var containerClient = blobServiceClient.GetBlobContainerClient(PROGRESS_CONTAINER);
            var blobClient = containerClient.GetBlobClient(PROGRESS_BLOB);
            
            using var ms = new MemoryStream(Encoding.UTF8.GetBytes(lineNumber.ToString()));
            await blobClient.UploadAsync(ms, overwrite: true);
        }

        private async IAsyncEnumerable<string> ReadBlobStorageFileAsync(
            string connectionString, 
            string containerName, 
            string filePath)
        {
            var blobServiceClient = new BlobServiceClient(connectionString);
            var containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            var blobClient = containerClient.GetBlobClient(filePath);

            List<string> lines = new();
            
            await using var response = await blobClient.OpenReadAsync();
            using var reader = new StreamReader(response);
            {
                string? line;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    lines.Add(line);
                }
            }

            foreach (var line in lines)
            {
                yield return line;
            }
        }

        private UserRating? ToUserRating(string str)
        {
            string[] strSplit = str.Split(",");
            try
            {
                return new UserRating
                {
                    UserId = Convert.ToInt32(strSplit[0]),
                    MovieId = Convert.ToInt32(strSplit[1]),
                    Rating = Convert.ToDouble(strSplit[2]),
                    Timestamp = NowUnixTimestamp()
                };
            }
            catch (Exception ex)
            {
                _logger.LogError($"An error occurred parsing UserRating data: {ex}");
                return null;
            }
        }

        private static int NowUnixTimestamp()
        {
            return (int)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;
        }
    }

    public class UserRating
    {
        public int UserId { get; set; }
        public int MovieId { get; set; }
        public double Rating { get; set; }
        public int Timestamp { get; set; }
    }
}