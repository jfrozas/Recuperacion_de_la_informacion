import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class TopTermsInDocs {
    public static void main(String[] args) throws IOException {
        String indexPath = "index";
        String[] parts = new String[2];
        int[] docID = new int[2];
        int top=0;
        String outfile = "outfile";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docID":
                    parts = args[++i].split("-");
                    docID[0] = Integer.parseInt(parts[0]);
                    docID[1] = Integer.parseInt(parts[1]);
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;
                case "-outfile":
                    outfile = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }
        Directory dir = null;
        DirectoryReader indexReader = null;
        Document doc = null;

        List<IndexableField> fields = null;

        File myObj = new File(outfile);
        myObj.createNewFile();
        FileWriter myWriter = new FileWriter(outfile);

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

        for (int i = 0; i < indexReader.numDocs(); i++) {
            if (i>=docID[0] && i<=docID[1]){
                try {
                    doc = indexReader.document(i);
                } catch (CorruptIndexException e1) {
                    System.out.println("Graceful message: exception " + e1);
                    e1.printStackTrace();
                } catch (IOException e1) {
                    System.out.println("Graceful message: exception " + e1);
                    e1.printStackTrace();
                }
                Terms vector = indexReader.getTermVector(i, "contents");
                TermsEnum termsEnum = null;
                termsEnum = vector.iterator();
                Map<String, Integer> frequencies = new HashMap<>();
                Map<String, Integer> idfs = new HashMap<>();
                Map<String, Double> formula = new HashMap<>();
                BytesRef text = null;
                while ((text = termsEnum.next()) != null) {
                    String term = text.utf8ToString();
                    int freq = (int) termsEnum.totalTermFreq();
                    idfs.put(term, indexReader.docFreq(new Term("contents",term)));
                    frequencies.put(term, freq);
                }
                Iterator<Entry<String, Integer>> set = idfs.entrySet().iterator();
                for (Map.Entry<String, Integer> entry : frequencies.entrySet()) {
                    Map.Entry<String, Integer> entry2 = set.next();
                    double result = entry.getValue() * Math.log10(indexReader.numDocs() / (double) entry2.getValue());
                    formula.put(entry.getKey(), result);
                }

                LinkedHashMap<String, Double> sortedMap = new LinkedHashMap<>();
                ArrayList<Double> list = new ArrayList<>();
                for (Map.Entry<String, Double> entry : formula.entrySet()) {
                    list.add(entry.getValue());
                }
                Collections.sort(list, new Comparator<Double>() {
                    public int compare(Double n, Double n2) {
                        if(n<n2){
                            return 1;
                        }else if(n>n2){
                            return -1;
                        }
                        return 0;
                    }
                });
                for (Double str : list) {
                    for (Entry<String, Double> entry : formula.entrySet()) {
                        if (entry.getValue().equals(str)) {
                            sortedMap.put(entry.getKey(), str);
                        }
                    }
                }
                int j = 0;
                String string = "-Document "+i;
                System.out.println("\n\n-Document "+i);
                for(Map.Entry<String, Double> entry : sortedMap.entrySet()){
                    if(j==top){
                        break;
                    }
                    string =string + "\n\nTerm -> "+entry.getKey()+"\ntf -> "+frequencies.get(entry.getKey())+"\ndf -> "+idfs.get(entry.getKey())+"\ntf x idflog10 -> "+entry.getValue();

                    System.out.println("\nTerm -> "+entry.getKey());
                    System.out.println("tf -> "+frequencies.get(entry.getKey()));
                    System.out.println("df -> "+idfs.get(entry.getKey()));
                    System.out.println("tf x idflog10 -> "+entry.getValue());

                    j++;
                }
                string = string+"\n\n\n";
                myWriter.write(string);
            }
        }
        myWriter.close();

    }
}
