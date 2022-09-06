using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using PCAxis.Paxiom;
using PCAxis.Paxiom.Extensions;
using Px.Search.Abstractions;
using System;
using System.Linq;

namespace Px.Search.Lucene.Legacy
{
    public class LuceneIndexer : IIndexer
    {
        private string _indexDirectory;
        private string _database;
        private IndexWriter _writer;
        private bool _running;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="indexDirectory">Index directory</param>
        /// <param name="database">Database id</param>
        public LuceneIndexer(string indexDirectory, string database)
        {
            _indexDirectory = indexDirectory;
            _database = database;

        }
        public void AddPaxiomDocument(string database, string id, string path, string table, string title, DateTime published, PXMeta meta)
        {
            Document doc = GetDocument(database, id, path, table, title, published, meta);

            _writer.AddDocument(doc);
        }

        public void UpdatePaxiomDocument(string database, string id, string path, string table, string title, DateTime published, PXMeta meta)
        {
            Document doc = GetDocument(database, id, path, table, title, published, meta);
            _writer.UpdateDocument(new Term(SearchConstants.SEARCH_FIELD_DOCID, doc.Get(SearchConstants.SEARCH_FIELD_DOCID)), doc);
        }

        public void Create(bool createIndex)
        {
            _writer = CreateIndexWriter(createIndex);
            if (createIndex)
            {
                _writer.SetMaxFieldLength(int.MaxValue);
            }
        }

        public void Dispose()
        {
            if (_running) {
                _writer.Rollback(); 
            }
            else {
                _writer.Optimize();
            }
            _writer.Dispose();
        }

        public void End()
        {
            _running = false;
        }

        /// <summary>
        /// Get Document object representing the table
        /// </summary>
        /// <param name="database">Database id</param>
        /// <param name="id">Id of document (table)</param>
        /// <param name="path">Path to table within database</param>
        /// <param name="path">Table</param>
        /// <param name="meta">PXMeta object</param>
        /// <returns>Document object representing the table</returns>
        private Document GetDocument(string database, string id, string path, string table, string title, DateTime published, PXMeta meta)
        {
            Document doc = new Document();

            if (meta != null)
            {
                if (string.IsNullOrEmpty(path) || string.IsNullOrEmpty(table) || string.IsNullOrEmpty(database) || string.IsNullOrEmpty(meta.Title) || string.IsNullOrEmpty(meta.Matrix) || meta.Variables.Count == 0)
                {
                    return doc;
                }

                doc.Add(new Field(SearchConstants.SEARCH_FIELD_DOCID, id, Field.Store.YES, Field.Index.NOT_ANALYZED)); // Used as id when updating a document - NOT searchable!!!
                doc.Add(new Field(SearchConstants.SEARCH_FIELD_SEARCHID, id, Field.Store.NO, Field.Index.ANALYZED)); // Used for finding a document by id - will be used for generating URL from just the tableid - Searchable!!!
                doc.Add(new Field(SearchConstants.SEARCH_FIELD_PATH, path, Field.Store.YES, Field.Index.NO));
                doc.Add(new Field(SearchConstants.SEARCH_FIELD_TABLE, table, Field.Store.YES, Field.Index.NO));
                doc.Add(new Field(SearchConstants.SEARCH_FIELD_DATABASE, database, Field.Store.YES, Field.Index.NOT_ANALYZED));
                doc.Add(new Field(SearchConstants.SEARCH_FIELD_PUBLISHED, published.DateTimeToPxDateString(), Field.Store.YES, Field.Index.NOT_ANALYZED));
                doc.Add(new Field(SearchConstants.SEARCH_FIELD_MATRIX, meta.Matrix, Field.Store.YES, Field.Index.ANALYZED));
                doc.Add(new Field(SearchConstants.SEARCH_FIELD_TITLE, title, Field.Store.YES, Field.Index.ANALYZED));
                doc.Add(new Field(SearchConstants.SEARCH_FIELD_VARIABLES, string.Join(" ", (from v in meta.Variables select v.Name).ToArray()), Field.Store.NO, Field.Index.ANALYZED));
                doc.Add(new Field(SearchConstants.SEARCH_FIELD_PERIOD, meta.GetTimeValues(), Field.Store.NO, Field.Index.ANALYZED));
                doc.Add(new Field(SearchConstants.SEARCH_FIELD_VALUES, meta.GetAllValues(), Field.Store.NO, Field.Index.ANALYZED));
                doc.Add(new Field(SearchConstants.SEARCH_FIELD_CODES, meta.GetAllCodes(), Field.Store.NO, Field.Index.ANALYZED));
                doc.Add(new Field(SearchConstants.SEARCH_FIELD_GROUPINGS, meta.GetAllGroupings(), Field.Store.NO, Field.Index.ANALYZED));
                doc.Add(new Field(SearchConstants.SEARCH_FIELD_GROUPINGCODES, meta.GetAllGroupingCodes(), Field.Store.NO, Field.Index.ANALYZED));
                doc.Add(new Field(SearchConstants.SEARCH_FIELD_VALUESETS, meta.GetAllValuesets(), Field.Store.NO, Field.Index.ANALYZED));
                doc.Add(new Field(SearchConstants.SEARCH_FIELD_VALUESETCODES, meta.GetAllValuesetCodes(), Field.Store.NO, Field.Index.ANALYZED));
                doc.Add(new Field(SearchConstants.SEARCH_FIELD_TABLEID, meta.TableID == null ? meta.Matrix : meta.TableID, Field.Store.YES, Field.Index.ANALYZED));
                if (!string.IsNullOrEmpty(meta.Synonyms))
                {
                    doc.Add(new Field(SearchConstants.SEARCH_FIELD_SYNONYMS, meta.Synonyms, Field.Store.NO, Field.Index.ANALYZED));
                }

            }

            return doc;
        }

        /// <summary>
        /// Get Lucene.Net IndexWriter object 
        /// </summary>
        /// <param name="createIndex">If index shall be created (true) or updated (false)</param>
        /// <returns>IndexWriter object. If the Index directory is locked, null is returned</returns>
        private IndexWriter CreateIndexWriter(bool createIndex)
        {
            FSDirectory fsDir = FSDirectory.Open(_indexDirectory);

            if (IndexWriter.IsLocked(fsDir))
            {
                return null;
            }

            IndexWriter writer = new IndexWriter(fsDir, new StandardAnalyzer(global::Lucene.Net.Util.Version.LUCENE_30), createIndex, IndexWriter.MaxFieldLength.LIMITED);
            return writer;
        }
    }
}
