using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace RedisDemo
{
    /// <summary>
    /// Based on Open Source Code - https://github.com/taswar/RedisForNetDevelopers
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            string connectionString =
                System.Configuration.ConfigurationManager.ConnectionStrings["RedisConnection"].ConnectionString;

            RedisStringKeyDemo(connectionString);
            RedisIncrementDecrement(connectionString);
            RedisHashesDemo(connectionString);
            RedisListDemo(connectionString);
            RedisSetDemo(connectionString);
            RedisSortedSet(connectionString);
            RedisPubSub(connectionString);
            RedisTransaction(connectionString);
            RedisBatchDemo(connectionString);

            Console.ReadKey();
        }

        private static void RedisBatchDemo(string connectionString)
        {
            var store = new RedisStore(connectionString);
            var redis = store.GetDatabase();

            RedisKey alphaKey = "alphaKey";
            RedisKey betaKey = "betaKey";

            redis.KeyDelete(alphaKey, CommandFlags.FireAndForget);
            redis.KeyDelete(betaKey, CommandFlags.FireAndForget);

            var writeTask = redis.StringSetAsync(alphaKey, "abc");
            var writeBetaTask = redis.StringSetAsync(betaKey, "beta");

            var readTask = redis.StringGetAsync(alphaKey);

            redis.Wait(writeTask);

            var readValue = redis.Wait(readTask);

            Console.WriteLine($"Redis Task wait and read {readValue}");

            writeBetaTask.Wait();

            readValue = redis.StringGet(betaKey);

            Console.WriteLine($"Task wait and read {readValue}");

            //Batching
            var list = new List<Task<bool>>();
            var keys = new List<RedisKey> { alphaKey, betaKey };
            IBatch batch = redis.CreateBatch();

            //add the delete into batch
            batch.KeyDeleteAsync(alphaKey);

            foreach (var key in keys)
            {
                var task = batch.StringSetAsync(key, "123");
                list.Add(task);
            }

            batch.Execute();

            Task.WhenAll(list.ToArray());

            readTask = redis.StringGetAsync(alphaKey);
            readValue = redis.Wait(readTask);
            Console.WriteLine($"Alpha read value {readValue}");

            readTask = redis.StringGetAsync(betaKey);
            readValue = redis.Wait(readTask);
            Console.WriteLine($"Beta read value {readValue}");

            store.Dispose();
        }

        private static void RedisTransaction(string connectionString)
        {
            var store = new RedisStore(connectionString);
            var redis = store.GetDatabase();

            RedisKey alphaKey = "alphaKey";
            RedisKey betaKey = "betaKey";
            RedisKey gammaKey = "gammaKey";

            redis.KeyDelete(alphaKey, CommandFlags.FireAndForget);
            redis.KeyDelete(betaKey, CommandFlags.FireAndForget);
            redis.KeyDelete(gammaKey, CommandFlags.FireAndForget);

            var trans = redis.CreateTransaction();

            var incr = trans.StringSetAsync(alphaKey, "abc");
            var exec = trans.ExecuteAsync();

            var result = redis.Wait(exec);
            var alphaValue = redis.StringGet(alphaKey);

            Console.WriteLine($"Alpha key is {alphaValue} and result is {result}");

            //using conditions to watch keys
            var condition = trans.AddCondition(Condition.KeyNotExists(gammaKey));
            var keyIncrement = trans.StringIncrementAsync(gammaKey);
            exec = trans.ExecuteAsync();
            result = redis.Wait(exec);

            var gammaValue = redis.StringGet(gammaKey);

            Console.WriteLine($"Gamma key is {gammaValue} and result is {result}");

            //fail condition
            condition = trans.AddCondition(Condition.KeyNotExists(gammaKey));
            keyIncrement = trans.StringIncrementAsync(gammaKey);
            exec = trans.ExecuteAsync();

            //resultis false 
            result = redis.Wait(exec);

            gammaValue = redis.StringGet(gammaKey);

            //value is still 1 and result is false
            Console.WriteLine($"Gamma key is {gammaValue} and result is {result}");

            store.Dispose();
        }

        private static void RedisPubSub(string connectionString)
        {
            var store = new RedisStore(connectionString);
            var redis = store.GetDatabase();

            var sub = redis.Multiplexer.GetSubscriber();

            //first subscribe, until we publish
            //subscribe to a test message
            sub.Subscribe("test", (channel, message) => {
                Console.WriteLine("Got notification: " + message);
            });

            //create a publisher
            var pub = redis.Multiplexer.GetSubscriber();

            //pubish to test channel a message
            var count = pub.Publish("test", "Hello there I am a test message");
            Console.WriteLine($"Number of listeners for test {count}");

            //pattern match with a message
            sub.Subscribe(new RedisChannel("a*c", RedisChannel.PatternMode.Pattern), (channel, message) => {
                Console.WriteLine($"Got pattern a*c notification: {message}");
            });

            count = pub.Publish("a*c", "Hello there I am a a*c message");
            Console.WriteLine($"Number of listeners for a*c {count}");

            pub.Publish("abc", "Hello there I am a abc message");
            pub.Publish("a1234567890c", "Hello there I am a a1234567890c message");
            pub.Publish("ab", "Hello I am a lost message"); //this mesage is never printed

            //Never a pattern match with a message
            sub.Subscribe(new RedisChannel("*123", RedisChannel.PatternMode.Literal), (channel, message) => {
                Console.WriteLine($"Got Literal pattern *123 notification: {message}");
            });

            pub.Publish("*123", "Hello there I am a *123 message");
            pub.Publish("a123", "Hello there I am a a123 message"); //message is never received due to literal pattern

            //Auto pattern match with a message
            sub.Subscribe(new RedisChannel("zyx*", RedisChannel.PatternMode.Auto), (channel, message) => {
                Console.WriteLine($"Got Literal pattern zyx* notification: {message}");
            });

            pub.Publish("zyxabc", "Hello there I am a zyxabc message");
            pub.Publish("zyx1234", "Hello there I am a zyxabc message");

            //no message being published to it so it will not receive any previous messages
            sub.Subscribe("test", (channel, message) => {
                Console.WriteLine($"I am a late subscriber Got notification: {message}");
            });

            sub.Unsubscribe("a*c");
            count = pub.Publish("abc", "Hello there I am a abc message"); //no one listening anymore
            Console.WriteLine($"Number of listeners for a*c {count}");

            store.Dispose();
        }

        private static void RedisSortedSet(string connectionString)
        {
            var store = new RedisStore(connectionString);
            var redis = store.GetDatabase();

            RedisKey topHackerKeys = "hackers";
            RedisKey alphaKey = "alphaKey";
            RedisKey destinationKey = "destKey";
            RedisKey intersectKey = "intersectKey";

            redis.KeyDelete(topHackerKeys, CommandFlags.FireAndForget);
            redis.KeyDelete(alphaKey, CommandFlags.FireAndForget);
            redis.KeyDelete(intersectKey, CommandFlags.FireAndForget);
            redis.KeyDelete(destinationKey, CommandFlags.FireAndForget);

            //According to http://www.arkhitech.com/12-greatest-programmers-of-all-time/
            var topProgrammers = new[] {
                "Dennis Ritchie",
                "Linus Torvalds",
                "Bjarne Stroustrup",
                "Tim Berners-Lee",
                "Brian Kernighan",
                "Donald Knuth",
                "Ken Thompson",
                "Guido van Rossum",
                "James Gosling",
                "Bill Gates",
                "Niklaus Wirth",
                "Ada Lovelace"
            };

            //add 12 items to the sorted set
            for (int i = 0, j = 1; i < topProgrammers.Length; i++, j++)
                redis.SortedSetAdd(topHackerKeys, topProgrammers[i], j);

            var members = redis.SortedSetScan(topHackerKeys);

            Console.WriteLine(string.Join(",\n", members));
            /* output 
             * Dennis Ritchie: 1, 
             * Linus Torvalds: 2,
             * Bjarne Stroustrup: 3,
             * Tim Berners-Lee: 4,
             * Brian Kernighan: 5,
             * Donald Knuth: 6,
             * Ken Thompson: 7,
             * Guido van Rossum: 8,
             * James Gosling: 9,
             * Bill Gates: 10,
             * Niklaus Wirth: 11,
             * Ada Lovelace: 12
            */

            Console.WriteLine(redis.SortedSetLength(topHackerKeys)); //output 12

            var byRanks = redis.SortedSetRangeByRank(topHackerKeys);
            Console.WriteLine(string.Join(",\n", byRanks));
            /* output 
            * Dennis Ritchie, 
            * Linus Torvalds,
            * Bjarne Stroustrup,
            * Tim Berners-Lee,
            * Brian Kernighan,
            * Donald Knuth,
            * Ken Thompson,
            * Guido van Rossum,
            * James Gosling,
            * Bill Gates,
            * Niklaus Wirth,
            * Ada Lovelace
           */

            Console.WriteLine(redis.SortedSetRank(topHackerKeys, "Linus Torvalds")); //output 1

            var byScore = redis.SortedSetRangeByScore(topHackerKeys, 1, topProgrammers.Length, Exclude.None, Order.Descending);
            Console.WriteLine(string.Join(",\n", byScore));
            /*output
             * Ada Lovelace,
             * Niklaus Wirth,
             * Bill Gates,
             * James Gosling,
             * Guido van Rossum,
             * Ken Thompson,
             * Donald Knuth,
             * Brian Kernighan,
             * Tim Berners-Lee,
             * Bjarne Stroustrup,
             * Linus Torvalds,
             * Dennis Ritchie
             */

            redis.SortedSetIncrement(topHackerKeys, "Linus Torvalds", 100);

            Console.WriteLine(redis.SortedSetScore(topHackerKeys, "Linus Torvalds")); //output 102, since it was 2 to being with

            redis.SortedSetDecrement(topHackerKeys, "Linus Torvalds", 100);

            Console.WriteLine(redis.SortedSetScore(topHackerKeys, "Linus Torvalds")); //output 2 back to original value

            redis.SortedSetAdd(alphaKey, "a", 1);
            redis.SortedSetAdd(alphaKey, "b", 1);
            redis.SortedSetAdd(alphaKey, "c", 1);

            redis.SortedSetCombineAndStore(SetOperation.Union, destinationKey, topHackerKeys, alphaKey);

            members = redis.SortedSetScan(destinationKey);
            Console.WriteLine("**********UNION**************");
            Console.WriteLine(string.Join(",\n", members));
            /* output
             * Dennis Ritchie: 1,
             * a: 1,
             * b: 1,
             * c: 1,
             * Linus Torvalds: 2,
             * Bjarne Stroustrup: 3,
             * Tim Berners-Lee: 4,
             * Brian Kernighan: 5,
             * Donald Knuth: 6,
             * Ken Thompson: 7,
             * Guido van Rossum: 8,
             * James Gosling: 9,
             * Bill Gates: 10,
             * Niklaus Wirth: 11,
             * Ada Lovelace: 12
             */

            redis.SortedSetCombineAndStore(SetOperation.Intersect, intersectKey, topHackerKeys, destinationKey);
            members = redis.SortedSetScan(intersectKey);

            //note it double the key scores
            Console.WriteLine("**********INTERSECT**************");
            Console.WriteLine(string.Join(",\n", members));
            /*output
             * Dennis Ritchie: 2,
             * Linus Torvalds: 4,
             * Bjarne Stroustrup: 6,
             * Tim Berners-Lee: 8,
             * Brian Kernighan: 10,
             * Donald Knuth: 12,
             * Ken Thompson: 14,
             * Guido van Rossum: 16,
             * James Gosling: 18,
             * Bill Gates: 20,
             * Niklaus Wirth: 22,
             * Ada Lovelace: 24
             */

            members = redis.SortedSetRangeByScoreWithScores(topHackerKeys, 2, 4);
            Console.WriteLine("**********RANGE BY SCORE WITH SCORES**************");
            Console.WriteLine(string.Join(",\n", members));
            /*output
             * Linus Torvalds: 2,
             * Bjarne Stroustrup: 3,
             * Tim Berners-Lee: 4
             */


            redis.SortedSetRemove(alphaKey, "a");
            members = redis.SortedSetScan(alphaKey);
            Console.WriteLine("**********REMOVE**************");
            Console.WriteLine(string.Join(",\n", members));
            /*output
             * b: 1,
             * c: 1
             */

            redis.SortedSetRemoveRangeByScore(destinationKey, 0, 11);
            members = redis.SortedSetScan(destinationKey);
            Console.WriteLine("**********REMOVE BY SCORE**************");
            Console.WriteLine(string.Join(",\n", members));
            /* output
             * Ada Lovelace: 12
             */

            store.Dispose();
        }

        private static void RedisSetDemo(string connectionString)
        {
            var store = new RedisStore(connectionString);
            var redis = store.GetDatabase();

            RedisKey key = "setKey";
            RedisKey alphaKey = "alphaKey";
            RedisKey numKey = "numberKey";
            RedisKey destinationKey = "destKey";

            redis.KeyDelete(key, CommandFlags.FireAndForget);
            redis.KeyDelete(alphaKey, CommandFlags.FireAndForget);
            redis.KeyDelete(numKey, CommandFlags.FireAndForget);
            redis.KeyDelete(destinationKey, CommandFlags.FireAndForget);

            //add 10 items to the set
            for (int i = 1; i <= 10; i++)
                redis.SetAdd(key, i);

            var members = redis.SetMembers(key);

            Console.WriteLine(string.Join(",", members)); //output 1,2,3,4,5,6,7,8,9,10

            //remove 5th element
            redis.SetRemove(key, 5);

            Console.WriteLine(redis.SetContains(key, 5)); //False

            members = redis.SetMembers(key);

            Console.WriteLine(string.Join(",", members)); //output 1,2,3,4,6,7,8,9,10

            Console.WriteLine(redis.SetContains(key, 10)); //True

            //number of elements
            Console.WriteLine(redis.SetLength(key)); //output 9

            //add alphabets to set
            redis.SetAdd(alphaKey, "abc".Select(x => (RedisValue)x.ToString()).ToArray());
            redis.SetAdd(numKey, "123".Select(x => (RedisValue)x.ToString()).ToArray());

            var values = redis.SetCombine(SetOperation.Union, numKey, alphaKey);

            Console.WriteLine(string.Join(",", values)); //unordered list of items (e.g output can be "1,3,2,a,c,b")

            values = redis.SetCombine(SetOperation.Difference, key, numKey);

            Console.WriteLine(string.Join(",", values)); //4, 6, 7, 8, 9, 10

            values = redis.SetCombine(SetOperation.Intersect, key, numKey);

            Console.WriteLine(string.Join(",", values)); //1, 2, 3

            //move a random from source numKey to aplhaKey 
            redis.SetMove(numKey, alphaKey, 2);

            members = redis.SetMembers(alphaKey);

            Console.WriteLine(string.Join(",", members)); //output can be (b, c, a, 2)

            //Add apple to 
            redis.SetAdd(alphaKey, "apple");

            //look for item that starts with he
            var patternMatchValues = redis.SetScan(alphaKey, "ap*"); //output apple

            Console.WriteLine(string.Join(",", patternMatchValues));

            patternMatchValues = redis.SetScan(alphaKey, "a*"); //output apple, a

            Console.WriteLine(string.Join(",", patternMatchValues));

            //store into destinantion key the union of numKey and alphaKey
            redis.SetCombineAndStore(SetOperation.Union, destinationKey, numKey, alphaKey);

            Console.WriteLine(string.Join(",", redis.SetMembers(destinationKey)));

            //radmon value and removes it from the set
            var randomVal = redis.SetPop(numKey);

            Console.WriteLine(randomVal); //random member in the set

            store.Dispose();
        }

        private static void RedisListDemo(string connectionString)
        {
            var store = new RedisStore(connectionString);
            var redis = store.GetDatabase();

            var listKey = "listKey";

            redis.KeyDelete(listKey, CommandFlags.FireAndForget);

            redis.ListRightPush(listKey, "a");

            var len = redis.ListLength(listKey);
            Console.WriteLine(len); //output  is 1

            redis.ListRightPush(listKey, "b");

            Console.WriteLine(redis.ListLength(listKey)); //putput is 2

            //lets clear it out
            redis.KeyDelete(listKey, CommandFlags.FireAndForget);

            redis.ListRightPush(listKey, "abcdefghijklmnopqrstuvwxyz".Select(x => (RedisValue)x.ToString()).ToArray());

            Console.WriteLine(redis.ListLength(listKey)); //output is 26

            Console.WriteLine(string.Concat(redis.ListRange(listKey))); //output is abcdefghijklmnopqrstuvwxyz

            var lastFive = redis.ListRange(listKey, -5);

            Console.WriteLine(string.Concat(lastFive)); //output vwxyz

            var firstFive = redis.ListRange(listKey, 0, 4);

            Console.WriteLine(string.Concat(firstFive)); //output abcde

            redis.ListTrim(listKey, 0, 1);

            Console.WriteLine(string.Concat(redis.ListRange(listKey))); //output ab

            //lets clear it out
            redis.KeyDelete(listKey, CommandFlags.FireAndForget);

            redis.ListRightPush(listKey, "abcdefghijklmnopqrstuvwxyz".Select(x => (RedisValue)x.ToString()).ToArray());

            var firstElement = redis.ListLeftPop(listKey);

            Console.WriteLine(firstElement); //output a, list is now bcdefghijklmnopqrstuvwxyz

            var lastElement = redis.ListRightPop(listKey);

            Console.WriteLine(lastElement); //output z, list is now bcdefghijklmnopqrstuvwxy

            redis.ListRemove(listKey, "c");

            Console.WriteLine(string.Concat(redis.ListRange(listKey))); //output is bdefghijklmnopqrstuvwxy   

            redis.ListSetByIndex(listKey, 1, "c");

            Console.WriteLine(string.Concat(redis.ListRange(listKey))); //output is bcefghijklmnopqrstuvwxy   

            var thirdItem = redis.ListGetByIndex(listKey, 3);

            Console.WriteLine(thirdItem); //output f  

            //lets clear it out
            var destinationKey = "destinationList";
            redis.KeyDelete(listKey, CommandFlags.FireAndForget);
            redis.KeyDelete(destinationKey, CommandFlags.FireAndForget);

            redis.ListRightPush(listKey, "abcdefghijklmnopqrstuvwxyz".Select(x => (RedisValue)x.ToString()).ToArray());

            var listLength = redis.ListLength(listKey);

            for (var i = 0; i < listLength; i++)
            {
                var val = redis.ListRightPopLeftPush(listKey, destinationKey);
                Console.Write(val);    //output zyxwvutsrqponmlkjihgfedcba
            }

            store.Dispose();
        }

        private static void RedisHashesDemo(string connectionString)
        {
            var store = new RedisStore(connectionString);
            var redis = store.GetDatabase();

            var hashKey = "hashKey";

            HashEntry[] redisBookHash =
            {
                new HashEntry("title", "Redis for .NET Developers"),
                new HashEntry("year", 2018),
                new HashEntry("author", "Ranjan Dailata")
            };

            redis.HashSet(hashKey, redisBookHash);

            if (redis.HashExists(hashKey, "year"))
            {
                var year = redis.HashGet(hashKey, "year"); //year is 2016
                Console.WriteLine(year);
            }

            var allHash = redis.HashGetAll(hashKey);

            //get all the items
            foreach (var item in allHash)
            {
                //output 
                //key: title, value: Redis for .NET Developers
                //key: year, value: 2018
                //key: author, value: Ranjan Dailata
                Console.WriteLine(string.Format("key : {0}, value : {1}", item.Name, item.Value));
            }

            //get all the values
            var values = redis.HashValues(hashKey);

            foreach (var val in values)
            {
                Console.WriteLine(val); //result = Redis for .NET Developers, 2018, Ranjan Dailata
            }

            //get all the keys
            var keys = redis.HashKeys(hashKey);

            foreach (var k in keys)
            {
                Console.WriteLine(k); //result = title, year, author
            }

            var len = redis.HashLength(hashKey);  //result of len is 3
            Console.WriteLine(len);

            if (redis.HashExists(hashKey, "year"))
            {
                var year = redis.HashIncrement(hashKey, "year", 1); 
                Console.WriteLine(year);

                var year2 = redis.HashDecrement(hashKey, "year", 1.5); 
                Console.WriteLine(year2);
            }

            store.Dispose();
        }

        private static void RedisIncrementDecrement(string connectionString)
        {
            var store = new RedisStore(connectionString);
            var redis = store.GetDatabase();

            var number = 1;
            var intKey = "integerKey";

            if (redis.StringSet(intKey, number))
            {
                //redis incr command
                var result = redis.StringIncrement(intKey); //after operation Our int number is now 102
                Console.WriteLine(result);

                //incrby command
                var newNumber = redis.StringIncrement(intKey, 100); // we now have incremented by 100, thus the new number is 202
                Console.WriteLine(newNumber);

                redis.KeyDelete("zeroValueKey");
                //by default redis stores a value of zero if no value is provided           
                var zeroValue = (int)redis.StringGet("zeroValueKey");
                Console.WriteLine(zeroValue);

                var someValue = (int)redis.StringIncrement("zeroValueKey"); //someValue is now 1 since it was incremented
                Console.WriteLine(someValue);

                //decr command
                redis.StringDecrement("zeroValueKey");
                someValue = (int)redis.StringGet("zeroValueKey"); //now someValue is back to 0   
                Console.WriteLine(someValue);

                //decrby command
                someValue = (int)redis.StringDecrement("zeroValueKey", 99); // now someValue is -99   
                Console.WriteLine(someValue);

                //append command
                redis.StringAppend("zeroValueKey", 1);
                someValue = (int)redis.StringGet("zeroValueKey"); //"Our zeroValueKey number is now -991   
                Console.WriteLine(someValue);

                redis.StringSet("floatValue", 1.1);
                var floatValue = (float)redis.StringIncrement("floatValue", 0.1); //fload value is now 1.2   
                Console.WriteLine(floatValue);
            }
            store.Dispose();
        }

        private static void RedisStringKeyDemo(string connectionString)
        {
            var store = new RedisStore(connectionString);
            var redis = store.GetDatabase();
            string key = "testKey";

            if (redis.StringSet(key, "testValue"))
            {
                var val = redis.StringGet(key);

                //output - StringGet(testKey) value is testValue
                Console.WriteLine("StringGet({0}) value is {1}", key, val);

                var v1 = redis.StringGetSet(key, "testValue2");

                //output - StringGetSet(testKey) testValue == testValue
                Console.WriteLine("StringGetSet({0}) {1} == {2}", key, val, v1);

                val = redis.StringGet(key);

                //output - StringGet(testKey) value is testValue2
                Console.WriteLine("StringGet({0}) value is {1}", key, val);

                //using SETNX 
                //code never goes into if since key already exist
                if (redis.StringSet(key, "someValue", TimeSpan.MaxValue, When.NotExists))
                {
                    val = redis.StringGet(key);
                    Console.WriteLine("StringGet({0}) value is {1}", key, val);
                }
                else
                {
                    //always goes here
                    Console.WriteLine("Value already exist");
                }

                var key2 = key + "1";
                if (redis.StringSet(key2, "someValue", TimeSpan.MaxValue, When.NotExists))
                {
                    val = redis.StringGet(key2);
                    //output - StringGet(testKey2) value is someValue", key2, val
                    Console.WriteLine("StringGet({0}) value is {1}", key2, val);
                }
                else
                {
                    //never goes here
                    Console.WriteLine("Value already exist");
                }
            }

            store.Dispose();
        }
    }
}
