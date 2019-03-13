package akka.contrib.persistence.mongodb

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicReference
import java.util.function.BinaryOperator

import com.typesafe.config.Config

import scala.collection.concurrent.TrieMap
import scala.util.{Success, Try}

trait MongoCollectionCache[C] {

  /**
    * Retrieve a collection from the cache if it exists or otherwise create it using an IDEMPOTENT procedure.
    *
    * @param collectionName    Name of the collection.
    * @param collectionCreator Creator of the collection. Must be idempotent.
    */
  def getOrElseCreate(collectionName: String, collectionCreator: String => C): C

  def invalidate(collectionName: String): Unit
}

object MongoCollectionCache {

  def apply[C](config: Config, path: String): MongoCollectionCache[C] = {
    val configuredCache =
      for {
        className <- Try(config.getString(s"$path.class"))
        constructor <- loadCacheConstructor[C](className)
      } yield constructor.apply(config.getConfig(path))

    configuredCache.getOrElse(createDefaultCache(config, path))
  }

  /**
    * Naive cache that retains cached collections forever.
    *
    * @tparam C Collection type.
    */
  case class Default[C]() extends MongoCollectionCache[C] {

    private[this] val trieMap: TrieMap[String, C] = TrieMap.empty[String, C]

    override def getOrElseCreate(collectionName: String, collectionCreator: String => C): C =
      trieMap.getOrElseUpdate(collectionName, collectionCreator.apply(collectionName))

    override def invalidate(collectionName: String): Unit =
      trieMap.remove(collectionName)
  }

  /**
    * Naive implementation of a cache whose entries expire after a period of time.
    * Memory consumption is not bounded.
    *
    * @param expireAfterWrite Duration that a cached collection remains valid.
    * @tparam C Collection type.
    */
  case class Expiring[C](expireAfterWrite: Duration) extends MongoCollectionCache[C] {

    private[this] val trieMap: TrieMap[String, (Instant, C)] = TrieMap.empty[String, (Instant, C)]

    override def getOrElseCreate(collectionName: String, collectionCreator: String => C): C = {
      val (createdAt, collection) =
        trieMap.getOrElseUpdate(collectionName, (Instant.now, collectionCreator.apply(collectionName)))
      val now = Instant.now
      if (createdAt.plus(expireAfterWrite).isBefore(now)) {
        val recreatedCollection = collectionCreator.apply(collectionName)
        trieMap.put(collectionName, (now, recreatedCollection))
        recreatedCollection
      } else {
        collection
      }
    }

    override def invalidate(collectionName: String): Unit =
      trieMap.remove(collectionName)
  }

  /**
    * Naive implementation of a cache that holds a single entry.
    *
    * @param config Cache configuration.
    * @tparam C Collection type.
    */
  case class Single[C](config: Config) extends MongoCollectionCache[C] {

    private[this] type CacheEntry = (Option[Instant], C)

    // protect cached collection by atomic reference
    // shouldn't be much slower than lazy val, which is not lock-free
    // consider using a lock-free queue of bounded size if this becomes a problem
    private[this] val box: AtomicReference[CacheEntry] = new AtomicReference()

    private[this] val expireAfterWrite: Option[Duration] = Try(config.getDuration("expire-after-write")).toOption

    override def getOrElseCreate(collectionName: String, collectionCreator: String => C): C =
      box.accumulateAndGet(null, JavaBinOp {
        case (entry, _) if isEntryValid(entry) => entry
        case _ => (expireAfterWrite.map(_ => Instant.now), collectionCreator(collectionName))
      })._2

    override def invalidate(collectionName: String): Unit =
      box.set(null)

    private[this] def isEntryValid(entry: CacheEntry): Boolean = {
      if (entry == null)
        false
      else {
        val didNotExpire =
          for {
            ttl <- expireAfterWrite
            expired = entry._1.forall(createdAt => createdAt.plus(ttl).isBefore(Instant.now))
          } yield !expired
        didNotExpire.getOrElse(true)
      }
    }

    private[this] case class JavaBinOp(scalaBinOp: (CacheEntry, CacheEntry) => CacheEntry) extends BinaryOperator[CacheEntry] {
      override def apply(t: CacheEntry, u: CacheEntry): CacheEntry = scalaBinOp(t, u)
    }

  }

  private[this] def loadCacheConstructor[C](className: String): Try[Config => MongoCollectionCache[C]] =
    for {
      nonEmptyClassName <- Success(className.trim).filter(_.nonEmpty)
      cacheClass <- Try(Class.forName(nonEmptyClassName))
      // if the loaded class implements MongoCollectionCache, take it on faith that it can store C
      if classOf[MongoCollectionCache[_]].isAssignableFrom(cacheClass)
      constructor <- getExpectedConstructor(cacheClass.asInstanceOf[Class[MongoCollectionCache[C]]])
    } yield constructor

  private[this] def getExpectedConstructor[T](cacheClass: Class[T]): Try[Config => T] =
    Try(cacheClass.getConstructor(classOf[Config])).map(constructor => x => constructor.newInstance(x))

  private[this] def createDefaultCache[C](config: Config, path: String): MongoCollectionCache[C] = {
    val maxSizePath = s"$path.max-size"
    val ttlPath = s"$path.expire-after-write"
    (Try(config.getConfig(path)), Try(config.getInt(maxSizePath)), Try(config.getDuration(ttlPath))) match {
      case (Success(cacheConfig), Success(1), _) => Single(cacheConfig)
      case (_, _, Success(ttl)) => Expiring(ttl)
      case _ => Default()
    }
  }
}
