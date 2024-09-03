using System.Text;

namespace org.apache.zookeeper.recipes.leader;

public static class Utils
{
    public static byte[] UTF8getBytes(this string str) {
        return Encoding.UTF8.GetBytes(str);
    }

    public static string UTF8bytesToString(this byte[] buffer)
    {
        return Encoding.UTF8.GetString(buffer, 0, buffer.Length);
    }
    
    public static string ToCommaDelimited<T>(this IEnumerable<T> enumerable)
    {
        return string.Join(",", enumerable);
    }
}