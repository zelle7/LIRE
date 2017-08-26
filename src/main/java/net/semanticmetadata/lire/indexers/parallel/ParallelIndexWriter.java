package net.semanticmetadata.lire.indexers.parallel;

import net.semanticmetadata.lire.utils.LuceneUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;

import java.io.IOException;
import java.util.List;

/**
 * @author zelle (christian@zellot.at)
 */
public class ParallelIndexWriter {

    IndexWriter indexWriter;

    public ParallelIndexWriter(String indexPath, boolean overWrite) throws IOException {
        indexWriter = LuceneUtils.createIndexWriter(indexPath, overWrite, LuceneUtils.AnalyzerType.WhitespaceAnalyzer);
    }

    /**
     * Will use the LuceneUtils to commit, optimize and close writer
     *
     * @see LuceneUtils#commitWriter(IndexWriter)
     * @see LuceneUtils#optimizeWriter(IndexWriter)
     * @see LuceneUtils#closeWriter(IndexWriter)
     * @throws IOException
     */
    public void finishedIndexing() throws IOException {
        LuceneUtils.commitWriter(indexWriter);
        LuceneUtils.optimizeWriter(indexWriter);
        LuceneUtils.closeWriter(indexWriter);
    }

    /**
     * @see LuceneUtils#commitWriter(IndexWriter)
     * wrapper function for the lucene index writer
     * @throws IOException
     */
    public void commitWriter() throws IOException {
        LuceneUtils.commitWriter(indexWriter);
    }

    /**
     * adds field to a document and adds the document to the index afterwards
     * @see Document#add(IndexableField)
     * @see IndexWriter#addDocument(Iterable)
     * @param doc
     * @param fields
     * @throws IOException
     */
    public void addDocument(Document doc, List<Field> fields) throws IOException {
        for (Field field : fields) {
            doc.add(field);
        }
        indexWriter.addDocument(doc);
    }

    /**
     * adds a document to the index
     * @see IndexWriter#addDocument(Iterable)
     * @param value
     * @throws IOException
     */
    public void addDocument(Document value) throws IOException {
        indexWriter.addDocument(value);
    }
}
