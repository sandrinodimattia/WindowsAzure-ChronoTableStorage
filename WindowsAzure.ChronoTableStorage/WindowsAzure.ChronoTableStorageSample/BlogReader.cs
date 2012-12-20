using System;
using System.Linq;
using System.Collections.Generic;
using System.ServiceModel.Syndication;

namespace WindowsAzure.ChronoTableStorageSample
{
    public static class BlogReader
    {
        public static IEnumerable<Model.BlogPost> Read()
        {
            using (var rdr = System.Xml.XmlReader.Create("http://blogs.msdn.com/b/windowsazure/rss.aspx"))
            {
                foreach (var item in SyndicationFeed.Load(rdr).Items)
                {
                    yield return new Model.BlogPost()
                    {
                        Author = item.ElementExtensions.FirstOrDefault(o => o.OuterName == "creator").GetObject<string>(),
                        PublishedOn = item.PublishDate.UtcDateTime,
                        Title = item.Title.Text
                    };
                }
            }
        }
    }
}
