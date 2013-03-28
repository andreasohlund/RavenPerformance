namespace RavenDBPerformance
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using Raven.Abstractions.Commands;
    using Raven.Client;

    public class RavenSagaPersister
    {
        public const string UniqueValueMetadataKey = "NServiceBus-UniqueValue";


        public IDocumentSession Session { get; set; }



        public void Save(ISagaEntity saga)
        {
            Session.Store(saga);
            StoreUniqueProperty(saga);
        }

        public void Update(ISagaEntity saga)
        {
            var p = UniqueAttribute.GetUniqueProperty(saga);

            if (!p.HasValue)
                return;

            var uniqueProperty = p.Value;

            var metadata = Session.Advanced.GetMetadataFor(saga);

            //if the user just added the unique property to a saga with existing data we need to set it
            if (!metadata.ContainsKey(UniqueValueMetadataKey))
            {
                StoreUniqueProperty(saga);
                return;
            }

            var storedvalue = metadata[UniqueValueMetadataKey].ToString();

            var currentValue = uniqueProperty.Value.ToString();

            if (currentValue == storedvalue)
                return;

            DeleteUniqueProperty(saga, new KeyValuePair<string, object>(uniqueProperty.Key, storedvalue));
            StoreUniqueProperty(saga);

        }

        public T Get<T>(Guid sagaId) where T : ISagaEntity
        {
            return Session.Load<T>(sagaId);
        }

        public T Get<T>(string property, object value) where T : ISagaEntity
        {
            if (IsUniqueProperty<T>(property))
                return GetByUniqueProperty<T>(property, value);

            return GetByQuery<T>(property, value).FirstOrDefault();
        }

        public void Complete(ISagaEntity saga)
        {
            Session.Delete(saga);

            var uniqueProperty = UniqueAttribute.GetUniqueProperty(saga);

            if (!uniqueProperty.HasValue)
                return;

            DeleteUniqueProperty(saga, uniqueProperty.Value);
        }

        bool IsUniqueProperty<T>(string property)
        {
            var key = typeof(T).FullName + property;
            bool value;

            if (!PropertyCache.TryGetValue(key, out value))
            {
                value = UniqueAttribute.GetUniqueProperties(typeof(T)).Any(p => p.Name == property);
                PropertyCache[key] = value;
            }

            return value;
        }


        T GetByUniqueProperty<T>(string property, object value) where T : ISagaEntity
        {
            var lookupId = SagaUniqueIdentity.FormatId(typeof(T), new KeyValuePair<string, object>(property, value));

            var lookup = Session
                .Include("SagaDocId") //tell raven to pull the saga doc as well to save us a roundtrip
                .Load<SagaUniqueIdentity>(lookupId);

            if (lookup != null)
                return lookup.SagaDocId != null
                           ? Session.Load<T>(lookup.SagaDocId) //if we have a saga id we can just load it
                           : Get<T>(lookup.SagaId); //if not this is a saga that was created pre 3.0.4 so we fallback to a get instead

            return default(T);
        }

        IEnumerable<T> GetByQuery<T>(string property, object value) where T : ISagaEntity
        {
            try
            {
                return Session.Advanced.LuceneQuery<T>()
                              .WhereEquals(property, value)
                              .WaitForNonStaleResultsAsOfNow();
            }
            catch (InvalidCastException)
            {
                return new[] { default(T) };
            }
        }

        void StoreUniqueProperty(ISagaEntity saga)
        {
            var uniqueProperty = UniqueAttribute.GetUniqueProperty(saga);

            if (!uniqueProperty.HasValue) return;

            var id = SagaUniqueIdentity.FormatId(saga.GetType(), uniqueProperty.Value);

            
            var sagaDocId = Session.Advanced.DocumentStore.Conventions.FindFullDocumentKeyFromNonStringIdentifier(saga.Id, saga.GetType(), false);

            Session.Store(new SagaUniqueIdentity
                {
                    Id = id,
                    SagaId = saga.Id,
                    UniqueValue = uniqueProperty.Value.Value,
                    SagaDocId = sagaDocId
                });

            SetUniqueValueMetadata(saga, uniqueProperty.Value);
        }

        void SetUniqueValueMetadata(ISagaEntity saga, KeyValuePair<string, object> uniqueProperty)
        {
            Session.Advanced.GetMetadataFor(saga)[UniqueValueMetadataKey] = uniqueProperty.Value.ToString();
        }

        void DeleteUniqueProperty(ISagaEntity saga, KeyValuePair<string, object> uniqueProperty)
        {
            var id = SagaUniqueIdentity.FormatId(saga.GetType(), uniqueProperty);

            Session.Advanced.Defer(new DeleteCommandData { Key = id });
        }

        static readonly ConcurrentDictionary<string, bool> PropertyCache = new ConcurrentDictionary<string, bool>();
    }
}