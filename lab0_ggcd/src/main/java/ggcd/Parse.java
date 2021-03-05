package ggcd;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class Parse {
    private String path;
    private ArrayList<String[]> titlesBasics;
    private ArrayList<String[]> titlePrincipals;
    private ArrayList<String[]> titleRatings;
    private ArrayList<String[]> nameBasics;

    public Parse(String path){
        this.path = path;
        this.titlesBasics = new ArrayList<>();
        this.titlePrincipals = new ArrayList<>();
        this.titleRatings = new ArrayList<>();
        this.nameBasics = new ArrayList<>();

    }

    public void parse(){
        parseTitlesBasics();
        parseTitlesPrincipals();
        parseTitlesRatings();
        parseNameBasics();
    }
    public void parseTitlesBasics(){
        long i = 0;
        CompressorStreamFactory csf = new CompressorStreamFactory();
        BufferedReader reader;
        try {
            reader = new BufferedReader( new InputStreamReader(
                    csf.createCompressorInputStream(new BufferedInputStream(new FileInputStream(this.path+"title.basics.tsv.bz2")))));
            String line = reader.readLine();

            while ((line = reader.readLine()) != null) {

                String[] auxLine = line.split("\t");
                this.titlesBasics.add(auxLine);

                i++;

            }

            System.out.println("Numero de linhas de title basics: " + i);
            reader.close();
        } catch (IOException | CompressorException e) {
            e.printStackTrace();
        }
    }

    public void parseTitlesPrincipals(){
        long i = 0;
        CompressorStreamFactory csf = new CompressorStreamFactory();
        BufferedReader reader;
        try {
            reader = new BufferedReader( new InputStreamReader(
                    csf.createCompressorInputStream(new BufferedInputStream(new FileInputStream(this.path+"title.principals.tsv.bz2")))));
            String line = reader.readLine();

            while ((line = reader.readLine()) != null) {

                String[] auxLine = line.split("\t");
                this.titlePrincipals.add(auxLine);

                i++;

            }

            System.out.println("Numero de linhas de title principals: " + i);
            reader.close();
        } catch (IOException | CompressorException e) {
            e.printStackTrace();
        }
    }

    public void parseTitlesRatings(){
        long i = 0;
        CompressorStreamFactory csf = new CompressorStreamFactory();
        BufferedReader reader;
        try {
            reader = new BufferedReader( new InputStreamReader(
                    csf.createCompressorInputStream(new BufferedInputStream(new FileInputStream(this.path+"title.ratings.tsv.bz2")))));
            String line = reader.readLine();

            while ((line = reader.readLine()) != null) {

                String[] auxLine = line.split("\t");
                this.titleRatings.add(auxLine);

                i++;

            }

            System.out.println("Numero de linhas de title ratings: " + i);
            reader.close();
        } catch (IOException | CompressorException e) {
            e.printStackTrace();
        }
    }

    public void parseNameBasics(){
        long i = 0;
        CompressorStreamFactory csf = new CompressorStreamFactory();
        BufferedReader reader;
        try {
            reader = new BufferedReader( new InputStreamReader(
                    csf.createCompressorInputStream(new BufferedInputStream(new FileInputStream(this.path+"name.basics.tsv.bz2")))));
            String line = reader.readLine();

            while ((line = reader.readLine()) != null) {

                String[] auxLine = line.split("\t");
                this.nameBasics.add(auxLine);

                i++;

            }

            System.out.println("Numero de linhas de name basics: " + i);
            reader.close();
        } catch (IOException | CompressorException e) {
            e.printStackTrace();
        }
    }

    public void topGenres(){
        ArrayList<String> genres = new ArrayList<>();
        ArrayList<String> uniqueGenres = new ArrayList<>();

        for(String[] row : this.titlesBasics){
            String[] aux = row[8].split(",");
            for(String s : aux) {
                genres.add(s);
            }
        }

        for(String row : genres){
            if(!uniqueGenres.contains(row)) uniqueGenres.add(row);
        }

        ArrayList<String> contagem = new ArrayList<String>();

        int j = 0;
        for(String aux : uniqueGenres){
            int count = 0;
            for (int i=0; i<genres.size(); i++) {
                if (aux.equals(genres.get(i))) {
                    count += 1;
                }
            }
            contagem.add(aux + " #"+count);
        }

        Collections.sort(contagem, new Comparator<String>(){
            public int compare(String s1, String s2){
                if(Integer.parseInt(s1.substring(s1.lastIndexOf("#")+1)) == Integer.parseInt(s2.substring(s2.lastIndexOf("#")+1)))
                    return 0;
                return Integer.parseInt(s1.substring(s1.lastIndexOf("#")+1)) > Integer.parseInt(s2.substring(s2.lastIndexOf("#")+1)) ? -1 : 1;
            }
        });
        System.out.println("\n\n\n TOP GENRES \n\n");
        int i = 1;
        for(String s : contagem){
            System.out.println(i + " : " + s);
            i++;
        }

    }

    public void listOfTitles(){
        ArrayList<String[]> aux = new ArrayList<>();

        for(String[] a : this.titlePrincipals){
            aux.add(a);
        }

        Collections.sort(aux, new Comparator<String[]>(){
            public int compare(String[] s1, String[] s2){
                if(Integer.parseInt(s1[2].substring(s1[2].lastIndexOf("m")+1)) == Integer.parseInt(s2[2].substring(s2[2].lastIndexOf("m")+1)))
                    return 0;
                return Integer.parseInt(s1[2].substring(s1[2].lastIndexOf("m")+1)) < Integer.parseInt(s2[2].substring(s2[2].lastIndexOf("m")+1)) ? -1 : 1;
            }
        });

        System.out.println("\n\nLIST OF TITLES PER PERSON (ORDERED BY PERSON IDENTIFIER)\n\n");
        for(String[] b : aux){

            System.out.println(b[0] +" | " + b[1]+" | " + b[2]+" | " + b[3]+" | " + b[4]+" | " + b[5]);
        }

    }
}
