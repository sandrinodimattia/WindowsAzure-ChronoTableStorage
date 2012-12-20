using System;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table.DataServices;

namespace WindowsAzure.ChronoTableStorage
{
    public static class DataServicesQueryExtensions
    {
        public static IQueryable<TTableEntity> Where<TTableEntity>(this IQueryable<TTableEntity> query, QueryDateReverseChronologicalComparisons comparison, DateTime date)
            where TTableEntity : TableServiceEntity
        {
            var key = "";
            switch (comparison)
            {
                case QueryDateReverseChronologicalComparisons.After:
                    key = RowKey.CreateReverseChronologicalKeyStart(date);
                    return query.Where(e => e.RowKey.CompareTo(key) < 0);
                case QueryDateReverseChronologicalComparisons.AfterOrEqual:
                    key = RowKey.CreateReverseChronologicalKeyStart(date.AddTicks(-1));
                    return query.Where(e => e.RowKey.CompareTo(key) < 0);
                case QueryDateReverseChronologicalComparisons.Before:
                    key = RowKey.CreateReverseChronologicalKeyStart(date.AddTicks(-1));
                    return query.Where(e => e.RowKey.CompareTo(key) > 0);
                case QueryDateReverseChronologicalComparisons.BeforeOrEqual:
                    key = RowKey.CreateReverseChronologicalKeyStart(date);
                    return query.Where(e => e.RowKey.CompareTo(key) > 0);
                default:
                    throw new InvalidOperationException("Unsupported comparison");
            }
        }

        public static IQueryable<TTableEntity> Where<TTableEntity>(this IQueryable<TTableEntity> query, QueryDateChronologicalComparisons comparison, DateTime date)
            where TTableEntity : TableServiceEntity
        {
            var key = "";
            switch (comparison)
            {
                case QueryDateChronologicalComparisons.After:
                    key = RowKey.CreateChronologicalKeyStart(date.AddTicks(1));
                    return query.Where(e => e.RowKey.CompareTo(key) > 0);
                case QueryDateChronologicalComparisons.AfterOrEqual:
                    key = RowKey.CreateChronologicalKeyStart(date);
                    return query.Where(e => e.RowKey.CompareTo(key) > 0);
                case QueryDateChronologicalComparisons.Before:
                    key = RowKey.CreateChronologicalKeyStart(date);
                    return query.Where(e => e.RowKey.CompareTo(key) < 0);
                case QueryDateChronologicalComparisons.BeforeOrEqual:
                    key = RowKey.CreateChronologicalKeyStart(date.AddTicks(1));
                    return query.Where(e => e.RowKey.CompareTo(key) < 0);
                default:
                    throw new InvalidOperationException("Unsupported comparison");
            }
        }
    }
}
