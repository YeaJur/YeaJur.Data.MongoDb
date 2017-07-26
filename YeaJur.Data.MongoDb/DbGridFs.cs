#region YeaJur.Data.MongoDb 4.0.30319.42000

/***
 *
 *	本代码版权归  侯兴鼎（YeaJur） 所有，All Rights Reserved (C) 2017
 * 	CLR版本：4.0.30319.42000
 *	唯一标识：29e6e8e5-a2d9-4ae4-8ec7-717f89cc04f4
 **
 *	所属域：DESKTOP-Q9MAAK4
 *	机器名称：DESKTOP-Q9MAAK4
 *	登录用户：houxi
 *	创建时间：2017/4/5 21:14:19
 *	作者：侯兴鼎（YeaJur）
 *	E_mail：houxingding@hotmail.com
 **
 *	命名空间：YeaJur.Data.MongoDb
 *	类名称：DbGridFs
 *	文件名：DbGridFs
 *	文件描述：
 *
 ***/

#endregion

using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;

namespace YeaJur.Data.MongoDb
{

    /// <summary>
    /// DbGridFs文件操作类
    /// </summary>
    public class DbGridFs
    {

        private IGridFSBucket Bucket { get; }

        /// <summary>
        /// 创建DbGridFs文件操作类
        /// </summary>
        /// <param name="dataBase">数据库对象</param>
        /// <param name="bucketOption"></param>
        public DbGridFs(IMongoDatabase dataBase, GridFSBucketOptions bucketOption)
        {
            Bucket = new GridFSBucket(dataBase, bucketOption);
        }

        #region Uploading Files

        /// <summary>
        /// 上传文件
        /// </summary>
        /// <param name="fileName">文件名称</param>
        /// <param name="source">文件源</param>
        /// <param name="options">上传选项</param>
        /// <returns>文件标识</returns>
        public ObjectId Upload(string fileName, byte[] source, GridFSUploadOptions options = null)
        {
            return Bucket.UploadFromBytes(fileName, source, options);
        }

        /// <summary>
        /// 异步上传文件
        /// </summary>
        /// <param name="fileName">文件名称</param>
        /// <param name="source">文件源</param>
        /// <param name="options">上传选项</param>
        /// <returns>文件标识</returns>
        public async Task<ObjectId> UploadAsync(string fileName, byte[] source, GridFSUploadOptions options = null)
        {
            return await Bucket.UploadFromBytesAsync(fileName, source, options);
        }

        /// <summary>
        /// 上传文件
        /// </summary>
        /// <param name="fileName">文件名称</param>
        /// <param name="source">文件源</param>
        /// <param name="options">上传选项</param>
        /// <returns>文件标识</returns>
        public ObjectId Upload(string fileName, Stream source, GridFSUploadOptions options = null)
        {
            return Bucket.UploadFromStream(fileName, source, options);
        }

        /// <summary>
        /// 异步上传文件
        /// </summary>
        /// <param name="fileName">文件名称</param>
        /// <param name="source">文件源</param>
        /// <param name="options">上传选项</param>
        /// <returns>文件标识</returns>
        public async Task<ObjectId> UploadAsync(string fileName, Stream source, GridFSUploadOptions options = null)
        {
            return await Bucket.UploadFromStreamAsync(fileName, source, options);
        }

        /// <summary>
        /// 打开并上传文件
        /// </summary>
        /// <param name="fileName">文件名称</param>
        /// <param name="source">文件源</param>
        /// <param name="options">上传选项</param>
        public void OpenUpload(string fileName, Stream source, GridFSUploadOptions options = null)
        {
            using (var stream = Bucket.OpenUploadStream(fileName, options))
            {
                var id = stream.Id; // the unique Id of the file being uploaded
                // write the contents of the file to stream using synchronous Stream methods
                stream.Close(); // optional because Dispose calls Close
            }
        }

        /// <summary>
        /// 异步打开并上传文件
        /// </summary>
        /// <param name="fileName">文件名称</param>
        /// <param name="source">文件源</param>
        /// <param name="options">上传选项</param>
        public async void OpenUploadAsync(string fileName, Stream source, GridFSUploadOptions options = null)
        {

            using (var stream = await Bucket.OpenUploadStreamAsync(fileName, options))
            {
                var id = stream.Id; // the unique Id of the file being uploaded
                // write the contents of the file to stream using asynchronous Stream methods
                await stream.CloseAsync(); // optional but recommended so Dispose does not block
            }
        }

        #endregion

        #region Downloading Files

        /// <summary>
        /// 下载文件
        /// </summary>
        /// <param name="id">文件标识</param>
        /// <param name="options">下载选项</param>
        /// <returns>文件源</returns>
        public byte[] Download(ObjectId id, GridFSDownloadOptions options = null)
        {
            return Bucket.DownloadAsBytes(id, options);
        }

        /// <summary>
        /// 异步下载文件
        /// </summary>
        /// <param name="id">文件标识</param>
        /// <param name="options">下载选项</param>
        /// <returns>文件源</returns>
        public async Task<byte[]> DownloadAsync(ObjectId id, GridFSDownloadOptions options = null)
        {
            return await Bucket.DownloadAsBytesAsync(id, options);
        }

        /// <summary>
        /// 下载文件
        /// </summary>
        /// <param name="fileName">文件名称</param>
        /// <param name="options">下载选项</param>
        /// <returns>文件源</returns>
        public byte[] Download(string fileName, GridFSDownloadByNameOptions options = null)
        {
            return Bucket.DownloadAsBytesByName(fileName, options);
        }

        /// <summary>
        /// 异步下载文件
        /// </summary>
        /// <param name="fileName">文件名称</param>
        /// <param name="options">下载选项</param>
        /// <returns>文件源</returns>
        public async Task<byte[]> DownloadAsync(string fileName, GridFSDownloadByNameOptions options = null)
        {
            return await Bucket.DownloadAsBytesByNameAsync(fileName, options);
        }

        /// <summary>
        /// 下载文件
        /// </summary>
        /// <param name="id">文件标识</param>
        /// <param name="source">下载文件源</param>
        /// <param name="options">下载选项</param>
        public void Download(ObjectId id, Stream source, GridFSDownloadOptions options = null)
        {
            Bucket.DownloadToStream(id, source, options);
        }

        /// <summary>
        /// 异步下载文件
        /// </summary>
        /// <param name="id">文件标识</param>
        /// <param name="source">下载文件源</param>
        /// <param name="options">下载选项</param>
        public async void DownloadAsync(ObjectId id, Stream source, GridFSDownloadOptions options = null)
        {
            await Bucket.DownloadToStreamAsync(id, source, options);
        }

        /// <summary>
        /// 下载文件
        /// </summary>
        /// <param name="fileName">文件名称</param>
        /// <param name="source">下载文件源</param>
        /// <param name="options">下载选项</param>
        /// <returns>文件源</returns>
        public void Download(string fileName, Stream source, GridFSDownloadByNameOptions options = null)
        {
            Bucket.DownloadToStreamByName(fileName, source, options);
        }

        /// <summary>
        /// 异步下载文件
        /// </summary>
        /// <param name="fileName">文件名称</param>
        /// <param name="source">下载文件源</param>
        /// <param name="options">下载选项</param>
        public async void DownloadAsync(string fileName, Stream source, GridFSDownloadByNameOptions options = null)
        {
            await Bucket.DownloadToStreamByNameAsync(fileName, source, options);
        }

        /// <summary>
        /// 打开并下载文件
        /// </summary>
        /// <param name="id">文件标识</param>
        /// <param name="source">下载文件源</param>
        /// <param name="options">下载选项</param>
        public void OpenDownload(ObjectId id, Stream source, GridFSDownloadOptions options = null)
        {
            using (var stream = Bucket.OpenDownloadStream(id, options))
            {
                // read from stream until end of file is reached
                stream.Close();
            }
        }

        /// <summary>
        /// 异步打开并下载文件
        /// </summary>
        /// <param name="id">文件标识</param>
        /// <param name="source">下载文件源</param>
        /// <param name="options">下载选项</param>
        public async void OpenDownloadAsync(ObjectId id, Stream source, GridFSDownloadOptions options = null)
        {
            using (var stream = await Bucket.OpenDownloadStreamAsync(id, options))
            {
                // read from stream until end of file is reached
                await stream.CloseAsync();
            }
        }

        /// <summary>
        /// 打开并下载文件
        /// </summary>
        /// <param name="fileName">文件名称</param>
        /// <param name="source">下载文件源</param>
        /// <param name="options">下载选项</param>
        public void OpenDownload(string fileName, Stream source, GridFSDownloadByNameOptions options = null)
        {
            using (var stream = Bucket.OpenDownloadStreamByName(fileName, options))
            {
                // read from stream until end of file is reached
                stream.Close();
            }
        }

        /// <summary>
        /// 异步打开并下载文件
        /// </summary>
        /// <param name="fileName">文件名称</param>
        /// <param name="source">下载文件源</param>
        /// <param name="options">下载选项</param>
        public async void OpenDownloadAsync(string fileName, Stream source, GridFSDownloadByNameOptions options = null)
        {
            using (var stream = await Bucket.OpenDownloadStreamByNameAsync(fileName, options))
            {
                // read from stream until end of file is reached
                await stream.CloseAsync();
            }
        }

        #endregion

        #region Finding Files

        /// <summary>
        /// 获取文件信息
        /// </summary>
        /// <param name="fileName">文件名称</param>
        /// <param name="filter">条件过滤器</param>
        /// <param name="options">查询选项</param>
        /// <returns>文件信息</returns>
        public GridFSFileInfo Find(string fileName, FilterDefinition<GridFSFileInfo> filter,
            GridFSFindOptions options = null)
        {
            //var filter = Builders<GridFSFileInfo>.Filter.And(
            //    Builders<GridFSFileInfo>.Filter.Eq(x => x.Filename, "securityvideo"),
            //    Builders<GridFSFileInfo>.Filter.Gte(x => x.UploadDateTime, new DateTime(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc)),
            //    Builders<GridFSFileInfo>.Filter.Lt(x => x.UploadDateTime, new DateTime(2015, 2, 1, 0, 0, 0, DateTimeKind.Utc)));
            //var sort = Builders<GridFSFileInfo>.Sort.Descending(x => x.UploadDateTime);
            //var options = new GridFSFindOptions
            //{
            //    Limit = 1,
            //    Sort = sort
            //};
            using (var cursor = Bucket.Find(filter, options))
            {
                return cursor.ToList().FirstOrDefault();
                // fileInfo either has the matching file information or is null
            }
        }

        /// <summary>
        /// 异步获取文件信息
        /// </summary>
        /// <param name="fileName">文件名称</param>
        /// <param name="filter">条件过滤器</param>
        /// <param name="options">查询选项</param>
        /// <returns>文件信息</returns>
        public async Task<GridFSFileInfo> FindAsync(string fileName, FilterDefinition<GridFSFileInfo> filter,
            GridFSFindOptions options = null)
        {

            using (var cursor = await Bucket.FindAsync(filter, options))
            {
                return (await cursor.ToListAsync()).FirstOrDefault();
                // fileInfo either has the matching file information or is null
            }
        }

        #endregion

        #region Deleting and Renaming Files

        /// <summary>
        /// 删除文件
        /// </summary>
        /// <param name="id">文件标识</param>
        public void Delete(ObjectId id)
        {
            Bucket.Delete(id);
        }

        /// <summary>
        /// 异步删除文件
        /// </summary>
        /// <param name="id">文件标识</param>
        public async void DeleteAsync(ObjectId id)
        {
            await Bucket.DeleteAsync(id);
        }

        /// <summary>
        /// 删除文档
        /// </summary>
        public void Drop()
        {
            Bucket.Drop();
        }

        /// <summary>
        /// 异步删除文档
        /// </summary>
        public async void DropAsync()
        {
            await Bucket.DropAsync();
        }

        /// <summary>
        /// 重命名
        /// </summary>
        /// <param name="newFilename">新文件名称</param>
        /// <param name="id">文件标识</param>
        public void Rename(string newFilename, ObjectId id)
        {
            Bucket.Rename(id, newFilename);
        }

        /// <summary>
        /// 异步重命名
        /// </summary>
        /// <param name="newFilename">新文件名称</param>
        /// <param name="id">文件标识</param>
        public async void RenameAsync(string newFilename, ObjectId id)
        {
            await Bucket.RenameAsync(id, newFilename);
        }

        /// <summary>
        /// 重命名
        /// </summary>
        /// <param name="oldFilename">旧文件名称</param>
        /// <param name="newFilename">新文件名称</param>
        public void Rename(string oldFilename, string newFilename)
        {
            var filter = Builders<GridFSFileInfo>.Filter.Eq(x => x.Filename, oldFilename);
            var filesCursor = Bucket.Find(filter);
            var files = filesCursor.ToList();

            foreach (var file in files)
            {
                Bucket.Rename(file.Id, newFilename);
            }
        }

        /// <summary>
        /// 异步重命名
        /// </summary>
        /// <param name="oldFilename">旧文件名称</param>
        /// <param name="newFilename">新文件名称</param>
        public async void RenameAsync(string oldFilename, string newFilename)
        {
            var filter = Builders<GridFSFileInfo>.Filter.Eq(x => x.Filename, oldFilename);
            var filesCursor = await Bucket.FindAsync(filter);
            var files = await filesCursor.ToListAsync();

            foreach (var file in files)
            {
                await Bucket.RenameAsync(file.Id, newFilename);
            }
        }

        #endregion
    }
}
