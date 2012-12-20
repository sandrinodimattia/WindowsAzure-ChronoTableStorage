using System;
using System.Linq;
using System.ServiceModel.Syndication;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using WindowsAzure.ChronoTableStorageSample.Model;
using WindowsAzure.ChronoTableStorage;

namespace WindowsAzure.ChronoTableStorageSample
{
    class Program
    {
        static void Main(string[] args)
        {
            // DisplayBlogPosts();
            // CreateDataWithIntegerRowKey();
            // CreateDataWithGuidRowKey();
            // CreateDataWithReverseChronologicalRowKey();
            // CreateDataWithReverseChronologicalRowKeyCustom();
            // CreateDataWithChronologicalRowKey();
            // ReverseChronologicalQueryAfter();
            DataServiceQuery();
            Console.Read();
        }

        static void DisplayBlogPosts()
        {
            foreach (var blogPost in BlogReader.Read())
            {
                Console.WriteLine("{0:yyyy-MM-dd}: {1}", blogPost.PublishedOn, blogPost.Title);
                Console.WriteLine("  -> Local Time: {0} - Universal Time: {1}", new DateTimeOffset(blogPost.PublishedOn).LocalDateTime, new DateTimeOffset(blogPost.PublishedOn).UtcDateTime);
            }
        }

        static void CreateDataWithIntegerRowKey()
        {
            var table = GetTable("ChronoTableStorageSample");

            int i = 0;
            foreach (var blogPost in BlogReader.Read())
            {
                table.Execute(TableOperation.Insert(new Model.BlogPostEntity()
                {
                    Author = blogPost.Author,
                    PartitionKey = "WindowsAzure",
                    PublishedOn = blogPost.PublishedOn,
                    Title = blogPost.Title,
                    RowKey = i++.ToString()
                }));
            }
        }

        static void CreateDataWithGuidRowKey()
        {
            var table = GetTable("ChronoTableStorageSample");
            
            foreach (var blogPost in BlogReader.Read())
            {
                table.Execute(TableOperation.Insert(new Model.BlogPostEntity()
                {
                    Author = blogPost.Author,
                    PartitionKey = "WindowsAzure",
                    PublishedOn = blogPost.PublishedOn,
                    Title = blogPost.Title,
                    RowKey = Guid.NewGuid().ToString()
                }));
            }
        }

        static void CreateDataWithReverseChronologicalRowKey()
        {
            var table = GetTable("ChronoTableStorageSample");

            foreach (var blogPost in BlogReader.Read())
            {
                table.Execute(TableOperation.Insert(new Model.BlogPostEntity()
                {
                    Author = blogPost.Author,
                    PartitionKey = "WindowsAzure",
                    PublishedOn = blogPost.PublishedOn,
                    Title = blogPost.Title,
                    RowKey = WindowsAzure.ChronoTableStorage.RowKey.CreateReverseChronological(blogPost.PublishedOn)
                }));
            }
        }

        static void CreateDataWithReverseChronologicalRowKeyCustom()
        {
            var table = GetTable("ChronoTableStorageSample");
            
            int i = 0;
            foreach (var blogPost in BlogReader.Read())
            {
                table.Execute(TableOperation.Insert(new Model.BlogPostEntity()
                {
                    Author = blogPost.Author,
                    PartitionKey = "WindowsAzure",
                    PublishedOn = blogPost.PublishedOn,
                    Title = blogPost.Title,
                    RowKey = WindowsAzure.ChronoTableStorage.RowKey.CreateReverseChronological(blogPost.PublishedOn, i++.ToString())
                }));
            }
        }

        static void CreateDataWithChronologicalRowKey()
        {
            var table = GetTable("ChronoTableStorageSample");

            foreach (var blogPost in BlogReader.Read())
            {
                table.Execute(TableOperation.Insert(new Model.BlogPostEntity()
                {
                    Author = blogPost.Author,
                    PartitionKey = "WindowsAzure",
                    PublishedOn = blogPost.PublishedOn,
                    Title = blogPost.Title,
                    RowKey = WindowsAzure.ChronoTableStorage.RowKey.CreateChronological(blogPost.PublishedOn)
                }));
            }
        }
                
        /// <summary>
        /// Call CreateDataWithReverseChronologicalRowKey first.
        /// </summary>
        static void ReverseChronologicalQueryAfter()
        {
            var table = GetTable("ChronoTableStorageSample");

            Console.WriteLine("Items between Dec. 10 and 12:");
            var queryItemsBetweenDates = new TableQuery<BlogPostEntity>()
                                .Where(TableQuery.CombineFilters(
                                            ChronologicalTableQuery.GenerateFilterCondition(QueryDateReverseChronologicalComparisons.AfterOrEqual, DateTime.Parse("2012-12-10 17:00:00")), TableOperators.And,
                                                    ChronologicalTableQuery.GenerateFilterCondition(QueryDateReverseChronologicalComparisons.BeforeOrEqual, DateTime.Parse("2012-12-12 18:00:00"))));
            foreach (var blogPost in table.ExecuteQuery(queryItemsBetweenDates))
            {
                Console.WriteLine("{0:yyyy-MM-dd}: {1}", blogPost.PublishedOn, blogPost.Title);
            }

            Console.WriteLine("Items after Dec. 10 at 00:00:00");
            var queryItemsAfterDate = new TableQuery<BlogPostEntity>()
                                .Where(ChronologicalTableQuery.GenerateFilterCondition(QueryDateReverseChronologicalComparisons.After, DateTime.Parse("2012-12-10 00:00:00")));
            foreach (var blogPost in table.ExecuteQuery(queryItemsAfterDate))
            {
                Console.WriteLine("{0:yyyy-MM-dd}: {1}", blogPost.PublishedOn, blogPost.Title);
            }

            Console.WriteLine("Items on Dec. 19:");
            var queryItemsForSpecificDate = new TableQuery<BlogPostEntity>()
                                .Where(TableQuery.CombineFilters(
                                            ChronologicalTableQuery.GenerateFilterCondition(QueryDateReverseChronologicalComparisons.AfterOrEqual, DateTime.Parse("2012-12-19 00:00:00")), TableOperators.And,
                                                    ChronologicalTableQuery.GenerateFilterCondition(QueryDateReverseChronologicalComparisons.Before, DateTime.Parse("2012-12-20 00:00:00"))));
            foreach (var blogPost in table.ExecuteQuery(queryItemsForSpecificDate))
            {
                Console.WriteLine("{0:yyyy-MM-dd}: {1}", blogPost.PublishedOn, blogPost.Title);
            }
        }

        static void DataServiceQuery()
        {
            var table = GetTable("ChronoTableStorageSample");

            var ctx = new Microsoft.WindowsAzure.Storage.Table.DataServices.TableServiceContext(table.ServiceClient);
            var query = ctx.CreateQuery<BlogPostTableServiceEntity>("ChronoTableStorageSample")
                           .Where(QueryDateReverseChronologicalComparisons.After, DateTime.Parse("2012-12-10 00:00:00"));

            foreach (var blogPost in query)
            {
                Console.WriteLine("{0:yyyy-MM-dd}: {1}", blogPost.PublishedOn, blogPost.Title);
            }
        }

        static CloudTable GetTable(string tableName)
        {
            var account = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=account;AccountKey=key");
            var tableClient = account.CreateCloudTableClient();
            var table = tableClient.GetTableReference(tableName);
            table.CreateIfNotExists();
            return table;
        }
    }
}
