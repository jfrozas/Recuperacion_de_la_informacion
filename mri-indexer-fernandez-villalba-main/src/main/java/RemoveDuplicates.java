import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class RemoveDuplicates {
    public static void main(String[] args) throws IOException {
        String indexPath = "index";
        String out = "out";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-out":
                    out = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }
        List<String> checksums = new ArrayList<>();

        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        IndexWriter writer = new IndexWriter(FSDirectory.open(Paths.get(out)), iwc);

        Directory dir = null;
        DirectoryReader indexReader = null;
        Document doc = null;

        List<IndexableField> fields = null;

        try {
            dir = FSDirectory.open(Paths.get(indexPath));
            indexReader = DirectoryReader.open(dir);
        } catch (CorruptIndexException e1) {
            System.out.println("Graceful message: exception " + e1);
            e1.printStackTrace();
        } catch (IOException e1) {
            System.out.println("Graceful message: exception " + e1);
            e1.printStackTrace();
        }
        System.out.println("Índice original: "+indexPath+"\nNúmero de documentos: "+indexReader.numDocs());

        for (int i = 0; i < indexReader.numDocs(); i++) {

            try {
                doc = indexReader.document(i);
            } catch (CorruptIndexException e1) {
                System.out.println("Graceful message: exception " + e1);
                e1.printStackTrace();
            } catch (IOException e1) {
                System.out.println("Graceful message: exception " + e1);
                e1.printStackTrace();
            }
            String chk = doc.getValues("checksum")[0];
            if(!checksums.contains(chk)){
                checksums.add(chk);
                writer.addDocument(doc);
            }
        }
        writer.commit();
        System.out.println("\nÍndice final: "+out+"\nNúmero de documentos: "+writer.getDocStats().numDocs);
    }
}
