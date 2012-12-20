using System;

namespace WindowsAzure.ChronoTableStorage
{
    public static class RowKey
    {
        public static string Separator = "-";

        private static long GetTicksChronological(DateTime dateTime)
        {
            return new DateTimeOffset(dateTime).UtcDateTime.Ticks;
        }

        private static long GetTicksDescending(DateTime dateTime)
        {
            return DateTimeOffset.MaxValue.UtcDateTime.Ticks - new DateTimeOffset(dateTime).UtcDateTime.Ticks;
        }

        private static string FormatKey(long ticks, string suffix)
        {
            return String.Format("{0:d21}{1}{2}", ticks, Separator, suffix);
        }

        public static string CreateChronological(DateTime dateTime)
        {
            return CreateChronological(dateTime, Guid.NewGuid().ToString("N").ToUpper());
        }

        public static string CreateChronologicalKeyStart(DateTime dateTime)
        {
            return CreateChronological(dateTime, "");
        }

        public static string CreateChronological(DateTime dateTime, string suffix)
        {
            return FormatKey(GetTicksChronological(dateTime), suffix);
        }

        public static string CreateReverseChronological(DateTime dateTime)
        {
            return CreateReverseChronological(dateTime, Guid.NewGuid().ToString("N").ToUpper());
        }

        public static string CreateReverseChronological(DateTime dateTime, string suffix)
        {
            return FormatKey(GetTicksDescending(dateTime), suffix);
        }

        public static string CreateReverseChronologicalKeyStart(DateTime dateTime)
        {
            return CreateReverseChronological(dateTime, "");
        }
    }
}
