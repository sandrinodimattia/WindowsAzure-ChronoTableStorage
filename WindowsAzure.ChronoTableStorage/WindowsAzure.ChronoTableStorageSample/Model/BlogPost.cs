using System;

using Microsoft.WindowsAzure.Storage.Table;

namespace WindowsAzure.ChronoTableStorageSample.Model
{
    public class BlogPost
    {
        public string Id { get; set; }

        public DateTime PublishedOn { get; set; }

        public string Author { get; set; }

        public string Title { get; set; }
    }
}
