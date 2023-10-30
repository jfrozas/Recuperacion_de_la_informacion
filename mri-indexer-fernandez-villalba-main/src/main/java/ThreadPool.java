import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * For the folder given as argument, the class SimpleThreadPool1
 * prints the name of each subfolder using a
 * different thread.
 */
public class ThreadPool {
    private final Path folder;
    private final IndexWriter writer;
    private final File indexesFile;
    private final Path mainIndex;
    private final float depth;
    private final boolean stored;
    private final boolean vector;
    private final boolean create;

    public ThreadPool(final Path folder, final IndexWriter writer, final File indexesFile, final Path mainIndex, final float depth, final boolean stored, final boolean vector, final boolean create) {
        this.folder = folder;
        this.writer = writer;
        this.indexesFile = indexesFile;
        this.mainIndex = mainIndex;
        this.depth = depth;
        this.stored = stored;
        this.vector = vector;
        this.create = create;
    }

    /**
     * This Runnable takes a folder and prints its path.
     */
    public static class WorkerThread implements Runnable {

        private final Path folder;
        private final File indexesFile;
        private final float depth;
        private final boolean stored;
        private final boolean vector;
        private final boolean create;
        private final Properties properties;
        private final boolean notFiles;
        private final boolean onlyFiles;
        private final boolean onlyLines;

        public WorkerThread(final Path folder, final File indexesFile, float depth, final boolean stored, final boolean vector, final boolean create, Properties properties,boolean notFiles,boolean onlyFiles,boolean onlyLines) {
            this.folder = folder;
            this.indexesFile = indexesFile;
            this.depth = depth;
            this.stored = stored;
            this.vector = vector;
            this.create = create;
            this.properties = properties;
            this.onlyFiles = onlyFiles;
            this.notFiles = notFiles;
            this.onlyLines = onlyLines;
        }

        /**
         * This is the work that the current thread will do when processed by the pool.
         * In this case, it will only print some information.
         */
        @Override
        public void run() {
            try {
                String indexes = indexesFile.getAbsolutePath();
                Analyzer analyzer = new StandardAnalyzer();
                IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
                IndexWriter writer2 = new IndexWriter(FSDirectory.open(Paths.get(indexes+"\\"+folder.getFileName().toString())), iwc);
                if (!create) {
                    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
                } else {
                    //writer2.deleteAll();
                    //writer2.commit();
                    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
                }
                System.out.println(Thread.currentThread().getName()+" started indexing the folder "+folder);
                indexDocs(writer2, folder);
                System.out.println(Thread.currentThread().getName()+" finished indexing the folder "+folder);
                writer2.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        void indexDocs(final IndexWriter writer, Path path) throws IOException {
            if (Files.isDirectory(path)) {
                Files.walkFileTree(
                        path,
                        new SimpleFileVisitor<>() {
                            float depth = WorkerThread.this.depth;
                            @Override
                            public FileVisitResult preVisitDirectory(Path dir,BasicFileAttributes attrs){
                                depth--;
                                if(depth==-1){
                                    return FileVisitResult.TERMINATE;
                                }else{
                                    return FileVisitResult.CONTINUE;
                                }
                            }
                            @Override
                            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                                int i = file.toString().lastIndexOf('.');
                                String fileEx = "."+file.toString().substring(i+1);
                                if(notFiles && properties.getProperty("notFiles").contains(fileEx)){
                                    return FileVisitResult.CONTINUE;
                                }
                                if(onlyFiles && !properties.getProperty("onlyFiles").contains(fileEx)){
                                    return FileVisitResult.CONTINUE;
                                }
                                try {
                                    indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
                                } catch (
                                        @SuppressWarnings("unused")
                                                IOException ignore) {
                                    ignore.printStackTrace(System.err);
                                    // don't index files that can't be read.
                                }
                                System.out.println("added "+file+" in depth "+depth);
                                return FileVisitResult.CONTINUE;
                            }
                        });
            } else {
                indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
            }
        }

        /** Indexes a single document */
        void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
            try (InputStream stream = Files.newInputStream(file)) {
                // make a new, empty document
                Document doc = new Document();

                Field pathField = new StringField("path", file.toString(), Field.Store.YES);
                doc.add(pathField);

                doc.add(new LongPoint("modified", lastModified));


                FieldType t = new FieldType();
                t.setTokenized(true);
                t.setStoreTermVectors(vector);
                t.setStored(stored);
                t.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
                String result = "";
                LineNumberReader reader = new LineNumberReader(new InputStreamReader(stream, "UTF-8"));
                int numLines = 2000000000;
                if(onlyLines){
                    numLines = Integer.parseInt(properties.getProperty("onlyLines"));
                }
                try{
                    String line;
                    while (((line = reader.readLine()) != null) && reader.getLineNumber() <= numLines) {
                        result = result +"\n"+ line;
                    }
                }finally{
                    reader.close();
                }

                Field contents = new Field("contents",result,t);

                //Field contents = new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8)));

                doc.add(contents);


                byte[] data = Files.readAllBytes(file);
                byte[] hash = MessageDigest.getInstance("MD5").digest(data);
                String checksum = new BigInteger(1, hash).toString(16);
                Field checksumField = new StringField("checksum", checksum, Field.Store.YES);
                doc.add(checksumField);


                Field hostnameField = new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES);
                doc.add(hostnameField);

                Field threadField = new StringField("thread", Thread.currentThread().getName(), Field.Store.YES);
                doc.add(threadField);

                Field typeField = new StringField("type", Files.probeContentType(file), Field.Store.YES);
                doc.add(typeField);

                Field sizeKbField = new StoredField("sizeKb", (float) Files.size(file)/1024);
                doc.add(sizeKbField);

                Field lastModifiedTimeField = new StringField("lastModifiedTime", String.valueOf(Files.getLastModifiedTime(file)), Field.Store.YES);
                doc.add(lastModifiedTimeField);

                BasicFileAttributes atr = Files.readAttributes(file, BasicFileAttributes.class);

                Field creationTimeField = new StringField("creationTime", String.valueOf(atr.creationTime()), Field.Store.YES);
                doc.add(creationTimeField);

                Field lastAccessTimeField = new StringField("lastAccessTime", String.valueOf(atr.lastAccessTime()), Field.Store.YES);
                doc.add(lastAccessTimeField);

                Field lastModifiedTimeLuceneField = new StringField("lastModifiedTimeLucene", DateTools.dateToString(new Date(Files.getLastModifiedTime(file).toMillis()), DateTools.Resolution.MILLISECOND), Field.Store.YES);
                doc.add(lastModifiedTimeLuceneField);

                Field creationTimeLuceneField = new StringField("creationTimeLucene", DateTools.dateToString(new Date(atr.creationTime().toMillis()), DateTools.Resolution.MILLISECOND), Field.Store.YES);
                doc.add(creationTimeLuceneField);

                Field lastAccessTimeLuceneField = new StringField("lastAccessTimeLucene", DateTools.dateToString(new Date(atr.lastAccessTime().toMillis()), DateTools.Resolution.MILLISECOND), Field.Store.YES);
                doc.add(lastAccessTimeLuceneField);

                if (writer.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE) {
                    // New index, so we just add the document (no old document can be there):
                    //System.out.println("adding " + file);
                    writer.addDocument(doc);
                } else {
                    // Existing index (an old copy of this document may have been indexed) so
                    // we use updateDocument instead to replace the old one matching the exact
                    // path, if present:
                    //System.out.println("updating " + file);
                    writer.updateDocument(new Term("path", file.toString()), doc);
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }


    }

    public void run(int numCores) throws IOException {
        final ExecutorService executor = Executors.newFixedThreadPool(numCores);

        //System.out.println("empieza el pool");

        /*
         * We use Java 7 NIO.2 methods for input/output management. More info in:
         * http://docs.oracle.com/javase/tutorial/essential/io/fileio.html
         *
         * We also use Java 7 try-with-resources syntax. More info in:
         * https://docs.oracle.com/javase/tutorial/essential/exceptions/
         * tryResourceClose.html
         */
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(this.folder)) {

            File confg = new File("D:\\datos\\UDC\\22-23\\segundoCuatrimestre\\RI\\pruebas\\src\\main\\resources\\config.properties");
            Properties properties = new Properties();

            properties.load(new BufferedReader(new InputStreamReader(Files.newInputStream(confg.toPath()), StandardCharsets.UTF_8)));
            boolean notFiles = false;
            boolean onlyFiles = false;
            boolean onlyLines = false;
            if(properties.getProperty("notFiles")!=null){
                notFiles = true;
            }
            if(properties.getProperty("onlyFiles")!=null){
                onlyFiles = true;
                notFiles = false;
            }
            if(properties.getProperty("onlyLines")!=null){
                onlyLines = true;
            }


            /* We process each subfolder in a new thread. */
            for (final Path path : directoryStream) {
                if (Files.isDirectory(path)) {

                    final Runnable worker = new WorkerThread(path, indexesFile, depth, stored,vector, create, properties, notFiles, onlyFiles, onlyLines);
                    /*
                     * Send the thread to the ThreadPool. It will be processed eventually.
                     */
                    executor.execute(worker);
                }
            }

        } catch (final IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        /*
         * Close the ThreadPool; no more jobs will be accepted, but all the previously
         * submitted jobs will be processed.
         */
        executor.shutdown();

        /* Wait up to 1 hour to finish all the previously submitted jobs */
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }
        /*if(create){
            writer.deleteAll();
            writer.commit();
        }*/
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(this.indexesFile.toPath())) {

            /* We process each subfolder in a new thread. */
            for (final Path path : directoryStream) {
                if (Files.isDirectory(path) && !path.equals(this.mainIndex)) {
                    this.writer.addIndexes(FSDirectory.open(path));
                    this.writer.commit();
                }
            }

        } catch (final IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        //System.out.println("Finished all threads");
    }


}