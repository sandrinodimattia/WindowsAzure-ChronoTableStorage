using System;

using Microsoft.WindowsAzure.Storage.Table.DataServices;

namespace WindowsAzure.ChronoTableStorageSample.Model
{
    public class BlogPostTableServiceEntity : TableServiceEntity
    {
        public DateTime PublishedOn { get; set; }

        public string Author { get; set; }

        public string Title { get; set; }
    }
}
