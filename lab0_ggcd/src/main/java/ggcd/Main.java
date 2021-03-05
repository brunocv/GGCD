package ggcd;

public class Main {
    public static void main(String args[]){

        long startTime = System.nanoTime();

        // micro < mini < original
        //Parse p = new Parse("Dados/micro/");
        //Parse p = new Parse("/home/bruno/IdeaProjects/lab0_ggcd/Dados/mini/");
        Parse p = new Parse("Dados/mini/");
        //Parse p = new Parse("Dados/original/");

        p.parse();
        p.topGenres();
        p.listOfTitles();
        long endTime = System.nanoTime();
        long duration = (endTime - startTime)/1000000; //miliseconds
        System.out.println("\n\nTIME: " + duration +"\n");
        /*
        java -Xmx3048m -jar target/lab0_ggcd-1.0-SNAPSHOT.jar
        "newgrp docker" em cada terminal para não usar sudo

        EXECUTION TIME FOR MICRO: 206 ms
        EXECUTION TIME FOR MINI: 5766 ms
        EXECUTION TIME FOR ORIGINAL: ardeu, nem com 12Gb em Xmx deu
         */
    }
}
