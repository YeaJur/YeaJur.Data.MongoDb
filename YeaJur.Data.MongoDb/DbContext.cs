#region YeaJur.Data.MongoDb 4.0.30319.42000

/***
 *
 *	本代码版权归  侯兴鼎（YeaJur） 所有，All Rights Reserved (C) 2017
 * 	CLR版本：4.0.30319.42000
 *	唯一标识：e4d11aea-7789-4144-8499-d37d29abf2a8
 **
 *	所属域：DESKTOP-Q9MAAK4
 *	机器名称：DESKTOP-Q9MAAK4
 *	登录用户：houxi
 *	创建时间：2017/4/5 21:11:47
 *	作者：侯兴鼎（YeaJur）
 *	E_mail：houxingding@hotmail.com
 **
 *	命名空间：YeaJur.Data.MongoDb
 *	类名称：DbContext
 *	文件名：DbContext
 *	文件描述：
 *
 ***/

#endregion

using MongoDB.Driver;
using System;

namespace YeaJur.Data.MongoDb
{
    /// <summary>
    /// MongoDB数据库上下文
    /// </summary>
    public class DbContext : IDisposable
    {

        /// <summary>
        /// MongoDb 数据库实例对象
        /// </summary>
        public IMongoDatabase DataBase { get; set; }

        /// <summary>
        /// MongoDB 客户端
        /// </summary>
        private IMongoClient Client { get; }

        /// <summary>
        /// MyDb 唯一实例
        /// </summary>
        private static DbContext _dbContext;
        /// <summary>
        /// 对象锁
        /// </summary>
        private static readonly object SyncObject = new object();

        /// <summary>
        /// 获取数据库实例对象
        /// </summary>
        /// <returns>数据库实例对象</returns>
        private DbContext(string connectionString, string dbName)
        {
            Client = new MongoClient(connectionString);
            DataBase = Client.GetDatabase(dbName);
        }

        public static DbContext Context(string connectionString, string dbName)
        {

            if (_dbContext != null) return _dbContext;
            lock (SyncObject)
            {
                return _dbContext ?? (_dbContext = new DbContext(connectionString, dbName));
            }
        }

        /// <summary>
        /// 回收实例
        /// </summary>
        public void Dispose()
        {
            //通知垃圾回收机制不再调用终结器（析构器）
            GC.SuppressFinalize(this);
        }
    }
}