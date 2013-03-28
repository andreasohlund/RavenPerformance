using System;

namespace RavenDBPerformance
{
    using System.Diagnostics;
    using System.Threading.Tasks;
    using System.Transactions;
    using Raven.Client;
    using Raven.Client.Document;

    class Program
    {
        static void Main(string[] args)
        {
            var numDocumentsToStore = 4000;
            var numThreads = 15;  //15 is the optimal value for nsb to pull messages from MSMQ
            var forceDtc = true;
            var useTheRealSagaPersister = false; // set this to true to run this test with the sagapersister nsb is using, false uses a simplefied version

            var fakeResourceManagerId = Guid.Parse("607b05af-c1fe-4afb-88d2-9736cd96b6ac");

            var documentStore = new DocumentStore
                {
                    Url = "http://localhost:8080",
                    DefaultDatabase = "Performance"
                };

            documentStore.Initialize();

            var sw = new Stopwatch();


            sw.Start();
            Parallel.For(0, numDocumentsToStore, new ParallelOptions { MaxDegreeOfParallelism = numThreads }, (i, s) =>
                {
                    using (var tx = new TransactionScope(TransactionScopeOption.Required, new TransactionOptions { IsolationLevel = IsolationLevel.ReadCommitted }))
                    {
                        if (forceDtc)
                        {
                            Transaction.Current.EnlistDurable(fakeResourceManagerId, new FakeMsmqResourceManager(),EnlistmentOptions.None);
                        }

                        using (var session = documentStore.OpenSession())
                        {
                            session.Advanced.AllowNonAuthoritativeInformation = false;
                            session.Advanced.UseOptimisticConcurrency = true;

                            if (useTheRealSagaPersister)
                                WithRealSagaPersister(session);
                            else
                                WithRawRavenCalls(session);


                            session.SaveChanges();
                        }


                        tx.Complete();
                    }

                });
            sw.Stop();

            Console.Out.WriteLine("Msg/s: {0}", numDocumentsToStore / sw.Elapsed.TotalSeconds);

            Console.ReadLine();
        }

        static void WithRealSagaPersister(IDocumentSession session)
        {
            var persister = new RavenSagaPersister
                {
                    Session = session
                };

             var orderId = Guid.NewGuid();

            persister.Get<SagaData>("OrderId", orderId);

            persister.Save(new SagaData{Id = Guid.NewGuid(),OrderId = orderId});
        }

        static void WithRawRavenCalls(IDocumentSession session)
        {
            var orderId = Guid.NewGuid();
            var sagaId = Guid.NewGuid();

            //fake a load since the saga persister will always load find existing sagas
            // the real persister does an include on the saga id
            session.Load<SagaUniqueDocument>(orderId);

            session.Store(new SagaData
                {
                    Id = Guid.NewGuid(),
                    OrderId = orderId
                });


            session.Store(new SagaUniqueDocument
                {
                    Id = orderId,
                    SagaId = sagaId
                });
            

            session.SaveChanges();
        }
    }

    internal class SagaData : ISagaEntity
    {
        public Guid Id { get; set; }
        public Guid OrderId { get; set; }
    }



    internal class SagaUniqueDocument : ISagaEntity
    {
        public Guid Id { get; set; }
        public Guid SagaId { get; set; }
    }
}
