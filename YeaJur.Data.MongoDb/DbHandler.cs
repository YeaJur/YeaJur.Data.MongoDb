#region YeaJur.Data.MongoDb 4.0.30319.42000

/***
 *
 *	本代码版权归  侯兴鼎（YeaJur） 所有，All Rights Reserved (C) 2017
 * 	CLR版本：4.0.30319.42000
 *	唯一标识：a6ae726e-4fac-43dd-bd0f-8968668dead3
 **
 *	所属域：DESKTOP-Q9MAAK4
 *	机器名称：DESKTOP-Q9MAAK4
 *	登录用户：houxi
 *	创建时间：2017/4/5 21:11:59
 *	作者：侯兴鼎（YeaJur）
 *	E_mail：houxingding@hotmail.com
 **
 *	命名空间：YeaJur.Data.MongoDb
 *	类名称：DbHandler
 *	文件名：DbHandler
 *	文件描述：
 *
 ***/

#endregion

using System.Collections.Generic;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace YeaJur.Data.MongoDb
{

    /// <summary>
    /// MongoDb数据库类
    /// </summary>
    public class DbHandler<TDocument>
    {
        /// <summary>
        /// MongoDb 数据库实例对象
        /// </summary>
        public IMongoDatabase DataBase { get; }
        /// <summary>
        /// 数据库集合对象集合
        /// </summary>
        private IMongoCollection<TDocument> Collection { get; }

        #region Document

        public DbHandler(string connectionString, string dbName)
        {
            DataBase = DbContext.Context(connectionString, dbName).DataBase;
            Collection = DataBase.GetCollection<TDocument>(typeof(TDocument).Name);
        }

        #region Insert Document 

        /// <summary>
        /// 异步插入一条记录
        /// </summary>
        /// <param name="model">数据对象</param>
        public async Task InsertAsync(TDocument model)
        {
            await Collection.InsertOneAsync(model);
        }

        /// <summary>
        /// 插入一条记录
        /// </summary>
        /// <param name="model">数据对象</param>
        public void Insert(TDocument model)
        {
            Collection.InsertOne(model);
        }

        /// <summary>
        /// 异步插入多条记录
        /// </summary>
        /// <param name="model">数据对象</param>
        public async Task
            InsertAsync(IEnumerable<TDocument> model)
        {
            await Collection.InsertManyAsync(model);
        }

        /// <summary>
        /// 插入多条记录
        /// </summary>
        /// <param name="model">数据对象</param>
        public void Insert(IEnumerable<TDocument> model)
        {
            Collection.InsertMany(model);
        }

        #endregion

        #region Update Document 

        /// <summary>
        /// 异步更新多条数据
        /// </summary>
        /// <param name="filter">过滤器</param>
        /// <param name="data">更新多条数据参数实体</param>
        /// <returns>更新结果</returns>
        public async Task<UpdateResult> UpdateAsync<T>(FilterDefinition<TDocument> filter, Dictionary<string, T> data)
        {
            var update = UpdateDefinitionBuilder(data);
            return await Collection.UpdateManyAsync(filter, update);
        }

        /// <summary>
        /// 更新多条数据
        /// </summary>
        /// <param name="filter">过滤器</param>
        /// <param name="data">更新多条数据集合</param>
        /// <returns>更新结果</returns>
        public UpdateResult Update(FilterDefinition<TDocument> filter, Dictionary<string, BsonValue> data)
        {
            var update = UpdateDefinitionBuilder(data);
            return Collection.UpdateMany(filter, update);
        }

        /// <summary>
        /// 更新定义构建器
        /// </summary>
        /// <param name="data">更新多条数据集合</param>
        /// <returns>更新定义</returns>
        private static UpdateDefinition<TDocument> UpdateDefinitionBuilder<T>(Dictionary<string, T> data)
        {
            var update = Builders<TDocument>.Update;
            foreach (var item in data)
            {
                update.Set(item.Key, item.Value);
            }
            return update.Combine();
        }

        /// <summary>
        /// 异步更新一条数据
        /// </summary>
        /// <param name="filter">过滤器</param>
        /// <param name="fieldName">字段名称</param>
        /// <param name="fieldValue">字段值</param>
        /// <returns>异步返回更新结果</returns>
        public async Task<UpdateResult> UpdateAsync(FilterDefinition<TDocument> filter, string fieldName,
            BsonValue fieldValue)
        {
            var update = Builders<TDocument>.Update.Set(fieldName, fieldValue);
            return await Collection.UpdateOneAsync(filter, update);
        }

        /// <summary>
        /// 更新一条数据
        /// </summary>
        /// <param name="filter">过滤器</param>
        /// <param name="fieldName">字段名称</param>
        /// <param name="fieldValue">字段值</param>
        /// <returns>更新结果</returns>
        public UpdateResult Update(FilterDefinition<TDocument> filter, string fieldName, BsonValue fieldValue)
        {
            var update = Builders<TDocument>.Update.Set(fieldName, fieldValue);
            return Collection.UpdateOne(filter, update);
        }

        /// <summary>
        /// 修改并返回实体
        /// </summary>
        /// <param name="filter">过滤器</param>
        /// <param name="fieldName">字段名称</param>
        /// <param name="fieldValue">字段值</param>
        /// <returns>异步返回更新实体</returns>
        public async Task<TDocument> FindOneAndUpdateAsync(FilterDefinition<TDocument> filter, string fieldName,
            BsonValue fieldValue)
        {
            var update = Builders<TDocument>.Update.Set(fieldName, fieldValue);
            return await Collection.FindOneAndUpdateAsync(filter, update);
        }

        /// <summary>
        /// 修改并返回实体
        /// </summary>
        /// <param name="filter">过滤器</param>
        /// <param name="fieldName">字段名称</param>
        /// <param name="fieldValue">字段值</param>
        /// <returns>返回更新实体</returns>
        public TDocument FindOneAndUpdate(FilterDefinition<TDocument> filter, string fieldName, BsonValue fieldValue)
        {
            var update = Builders<TDocument>.Update.Set(fieldName, fieldValue);
            return Collection.FindOneAndUpdate(filter, update);
        }

        /// <summary>
        /// 替换并返回实体
        /// </summary>
        /// <param name="filter">替换过滤器</param>
        /// <param name="model">替换实体</param>
        /// <returns>异步返回替换实体</returns>
        public async Task<TDocument> FindOneAndReplaceAsync(FilterDefinition<TDocument> filter, TDocument model)
        {
            return await Collection.FindOneAndReplaceAsync(filter, model);
        }

        /// <summary>
        /// 替换并返回实体
        /// </summary>
        /// <param name="filter">替换过滤器</param>
        /// <param name="model">替换实体</param>
        /// <returns>替换实体</returns>
        public TDocument FindOneAndReplace(FilterDefinition<TDocument> filter, TDocument model)
        {
            return Collection.FindOneAndReplace(filter, model);
        }

        #endregion

        #region Find or Query Document

        /// <summary>
        /// 获取所有文档
        /// </summary>
        /// <returns>文档集合</returns>
        public async Task<List<TDocument>> GetAllDocument()
        {
            var list = new List<TDocument>();
            var filter = new BsonDocument();
            using (var cursor = await Collection.FindAsync(filter))
            {
                while (await cursor.MoveNextAsync())
                {
                    list.AddRange(cursor.Current);
                }
            }
            return list;
        }

        /// <summary>
        /// 根据查询条件获取一条数据
        /// </summary>
        /// <param name="filter">过滤器</param>
        /// <returns></returns>
        public IAsyncCursor<TDocument> FindSync(FilterDefinition<TDocument> filter)
        {
            return Collection.FindSync(filter);
        }

        /// <summary>
        /// 根据查询条件获取一条数据
        /// </summary>
        /// <param name="filter">过滤器</param>
        /// <returns></returns>
        public async Task<IAsyncCursor<TDocument>> FindAsync(FilterDefinition<TDocument> filter)
        {
            return await Collection.FindAsync(filter);
        }

        /// <summary>
        /// 根据查询条件获取多条数据
        /// </summary>
        /// <param name="filter">过滤器</param>
        /// <returns></returns>
        public IFindFluent<TDocument, TDocument> Find(FilterDefinition<TDocument> filter)
        {
            return Collection.Find(filter);
        }

        /// <summary>
        /// 分页查询 指定索引最后项-PageSize模式 
        /// </summary>
        /// <param name="query">查询的条件 没有可以为null</param>
        /// <param name="indexName">索引名称</param>
        /// <param name="lastKeyValue">最后索引的值</param>
        /// <param name="pageSize">分页的尺寸</param>
        /// <param name="sortJson">排序json</param>
        /// <returns>返回一个List列表数据</returns>
        public List<TDocument> FindPage(FilterDefinition<TDocument> query, string indexName, object lastKeyValue,
            int pageSize, string sortJson)
        {
            var queryBuilder = new FilterDefinitionBuilder<TDocument>();
            if (string.IsNullOrEmpty(indexName))
            {
                indexName = "_id";
            }
            var mongoCursor =
                Collection.Find(queryBuilder.And(query, queryBuilder.Lt(indexName, BsonValue.Create(lastKeyValue))))
                    .Sort(sortJson)
                    .Limit(pageSize);
            return mongoCursor.ToList();
        }

        /// <summary>
        /// 分页查询 指定索引最后项-PageSize模式 
        /// </summary>
        /// <param name="query">查询的条件 没有可以为null</param>
        /// <param name="indexName">索引名称</param>
        /// <param name="lastKeyValue">最后索引的值</param>
        /// <param name="pageSize">分页的尺寸</param>
        /// <param name="sortJson">排序json</param>
        /// <returns>返回一个List列表数据</returns>
        public async Task<List<TDocument>> FindPageAsync(FilterDefinition<TDocument> query, string indexName,
            object lastKeyValue, int pageSize, string sortJson)
        {
            var queryBuilder = new FilterDefinitionBuilder<TDocument>();
            if (string.IsNullOrEmpty(indexName))
            {
                indexName = "_id";
            }
            var mongoCursor =
                Collection.Find(queryBuilder.And(query, queryBuilder.Lt(indexName, BsonValue.Create(lastKeyValue))))
                    .Sort(sortJson)
                    .Limit(pageSize);
            return await mongoCursor.ToListAsync();
        }

        #endregion

        #region Delete Document

        /// <summary>
        /// 删除集合中符合条件的数据
        /// </summary>
        /// <param name="filter">过滤器</param>
        /// <returns></returns>
        public DeleteResult Delete(FilterDefinition<TDocument> filter)
        {
            return Collection.DeleteOne(filter);
        }

        /// <summary>
        /// 删除集合中符合条件的数据
        /// </summary>
        /// <param name="filter">过滤器</param>
        /// <returns></returns>
        public async Task<DeleteResult> DeleteAsync(FilterDefinition<TDocument> filter)
        {
            return await Collection.DeleteOneAsync(filter);
        }

        /// <summary>
        /// 删除集合中符合条件的数据
        /// </summary>
        /// <param name="filter">过滤器</param>
        /// <returns></returns>
        public DeleteResult Deletes(FilterDefinition<TDocument> filter)
        {
            return Collection.DeleteMany(filter);
        }

        /// <summary>
        /// 删除集合中符合条件的数据
        /// </summary>
        /// <param name="filter">过滤器</param>
        /// <returns></returns>
        public async Task<DeleteResult> DeletesAsync(FilterDefinition<TDocument> filter)
        {
            return await Collection.DeleteManyAsync(filter);
        }

        /// <summary>
        /// 删除集合中的所有数据
        /// </summary>
        public DeleteResult DeleteAll()
        {
            return Collection.DeleteMany(new BsonDocument());
        }

        /// <summary>
        /// 删除集合中的所有数据
        /// </summary>
        public async Task<DeleteResult> DeleteAllAsync()
        {
            return await Collection.DeleteManyAsync(new BsonDocument());
        }


        /// <summary>
        /// 删除并返回实体
        /// </summary>
        /// <param name="filter"></param>
        /// <returns></returns>
        public TDocument FindOneAndDelete(FilterDefinition<TDocument> filter)
        {
            return Collection.FindOneAndDelete(filter);
        }

        /// <summary>
        /// 删除并返回实体
        /// </summary>
        /// <param name="filter"></param>
        /// <returns></returns>
        public async Task<TDocument> FindOneAndDeleteAsync(FilterDefinition<TDocument> filter)
        {
            return await Collection.FindOneAndDeleteAsync(filter);
        }

        #endregion

        #region Document Indexees

        /// <summary>
        /// 创建单字段索引
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        public async Task<string> CreateSingleFieldIndexAsync(FieldDefinition<TDocument> field)
        {
            var keys = Builders<TDocument>.IndexKeys.Ascending(field);
            return await Collection.Indexes.CreateOneAsync(keys);
        }

        /// <summary>
        /// 创建单字段索引
        /// </summary>
        /// <param name="field">多字段索引实体</param>
        /// <returns></returns>
        public string CreateSingleFieldIndex(FieldDefinition<TDocument> field)
        {
            var keys = Builders<TDocument>.IndexKeys.Ascending(field);
            return Collection.Indexes.CreateOne(keys);
        }

        /// <summary>
        /// 创建多字段索引
        /// </summary>
        /// <param name="field">多字段索引实体</param>
        /// <returns></returns>
        public IEnumerable<string> CreateManyFieldIndex(IEnumerable<CreateIndexModel<TDocument>> field)
        {
            return Collection.Indexes.CreateMany(field);
        }

        /// <summary>
        /// 创建多字段索引
        /// </summary>
        /// <param name="field">多字段索引实体</param>
        /// <returns></returns>
        public async Task<IEnumerable<string>> CreateManyFieldIndexAsync(IEnumerable<CreateIndexModel<TDocument>> field)
        {
            return await Collection.Indexes.CreateManyAsync(field);
        }

        /// <summary>
        /// 删除一个索引
        /// </summary>
        /// <param name="indexFieldName">索引名称</param>
        public async void DropOneIndexAsync(string indexFieldName)
        {
            await Collection.Indexes.DropOneAsync(indexFieldName);
        }

        /// <summary>
        /// 删除一个索引
        /// </summary>
        /// <param name="indexFieldName">索引名称</param>
        public void DropOneIndex(string indexFieldName)
        {
            Collection.Indexes.DropOne(indexFieldName);
        }

        /// <summary>
        /// 删除所有索引
        /// </summary>
        public async void DropAllIndexAsync()
        {
            await Collection.Indexes.DropAllAsync();
        }


        /// <summary>
        /// 删除所有索引
        /// </summary>
        public void DropAllIndex()
        {
            Collection.Indexes.DropAll();
        }

        #endregion

        #endregion
    }
}