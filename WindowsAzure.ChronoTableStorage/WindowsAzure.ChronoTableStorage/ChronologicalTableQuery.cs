using System;

using Microsoft.WindowsAzure.Storage.Table;

namespace WindowsAzure.ChronoTableStorage
{
    public static class ChronologicalTableQuery
    {
        public static string GenerateFilterCondition(QueryDateReverseChronologicalComparisons comparison, DateTime date)
        {
            var key = "";
            var queryComparison = QueryComparisons.Equal;

            switch (comparison)
            {
                case QueryDateReverseChronologicalComparisons.After:
                    key = RowKey.CreateReverseChronologicalKeyStart(date);
                    queryComparison = QueryComparisons.LessThan;
                    break;
                case QueryDateReverseChronologicalComparisons.AfterOrEqual:
                    key = RowKey.CreateReverseChronologicalKeyStart(date.AddTicks(-1));
                    queryComparison = QueryComparisons.LessThan;
                    break;
                case QueryDateReverseChronologicalComparisons.Before:
                    queryComparison = QueryComparisons.GreaterThan;
                    key = RowKey.CreateReverseChronologicalKeyStart(date.AddTicks(-1));
                    break;
                case QueryDateReverseChronologicalComparisons.BeforeOrEqual:
                    queryComparison = QueryComparisons.GreaterThan;
                    key = RowKey.CreateReverseChronologicalKeyStart(date);
                    break;
                default:
                    break;
            }

            return TableQuery.GenerateFilterCondition("RowKey", queryComparison, key);
        }

        public static string GenerateFilterCondition(QueryDateChronologicalComparisons comparison, DateTime date)
        {
            var key = "";
            var queryComparison = QueryComparisons.Equal;

            switch (comparison)
            {
                case QueryDateChronologicalComparisons.After:
                    key = RowKey.CreateChronologicalKeyStart(date.AddTicks(1));
                    queryComparison = QueryComparisons.GreaterThan;
                    break;
                case QueryDateChronologicalComparisons.AfterOrEqual:
                    key = RowKey.CreateChronologicalKeyStart(date);
                    queryComparison = QueryComparisons.GreaterThan;
                    break;
                case QueryDateChronologicalComparisons.Before:
                    queryComparison = QueryComparisons.LessThan;
                    key = RowKey.CreateChronologicalKeyStart(date);
                    break;
                case QueryDateChronologicalComparisons.BeforeOrEqual:
                    queryComparison = QueryComparisons.LessThan;
                    key = RowKey.CreateChronologicalKeyStart(date.AddTicks(1));
                    break;
                default:
                    break;
            }

            return TableQuery.GenerateFilterCondition("RowKey", queryComparison, key);
        }
    }
}
