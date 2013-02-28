using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Intercom.Discovery
{
    /// <summary>
    /// Extension methods for ZRE
    /// </summary>
    static class ZreExtensions
    {
        /// <summary>
        /// Parses an UUID from a byte sequence
        /// </summary>
        /// <param name="payload">The payload.</param>
        /// <param name="startIndex">The start index.</param>
        /// <returns></returns>
        public static Guid ParseGuid(this byte[] payload, int startIndex)
        {
            if (payload == null) throw new ArgumentNullException("payload");
            if (payload.Length - startIndex < 16) throw new ArgumentException("Payload must be at least 16 bytes long");

            int a = BitConverter.ToInt32(payload, startIndex);
            short b = BitConverter.ToInt16(payload, startIndex + 4);
            short c = BitConverter.ToInt16(payload, startIndex + 6);
            byte d = payload[startIndex + 8],
                 e = payload[startIndex + 9],
                 f = payload[startIndex + 10],
                 g = payload[startIndex + 11],
                 h = payload[startIndex + 12],
                 i = payload[startIndex + 13],
                 j = payload[startIndex + 14],
                 k = payload[startIndex + 15];
            return new Guid(a, b, c, d, e, f, g, h, i, j, k);
        }
    }
}
