﻿using System;
using System.Diagnostics;
using System.Threading;
using Intercom.Discovery;

namespace Intercom
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var zre = new ZreMailbox())
            {
                // Starten
                if (!zre.Start())
                {
                    Trace.TraceError("Could not start ZRE Discovery.");
                    return;
                }

                // Broadcast senden
                Console.WriteLine("Key to send ...");
                Console.ReadKey(true);
                zre.BroadcastBeacon();

                // Nettigkeit
                Console.WriteLine("Key to exit ...");
                Console.ReadKey(true);

                // Sachsen-Anhalt
                zre.Stop();
            }
        }
    }
}
