package br.unicamp.ic.zab.apptest;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

import br.unicamp.ic.zab.Callback;
import br.unicamp.ic.zab.Zab;

public class AppTestNoRecover {




    /**
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {

        Random rnd = new Random (System.currentTimeMillis());

        PrintWriter logfile = null ;
        try{
            long myId = Integer.parseInt(args[0]);


            logfile = new PrintWriter(new FileWriter("log"+myId+".txt"));
            logfile.println("START LOG");

            final PrintWriter log = logfile;
            Callback callback = new Callback(){
                @Override
                public void deliver(byte[] payload) {
                   log.println(new String(payload));

                }
            };

             Zab zab = new Zab(myId,callback);
             zab.start();

             Thread.sleep(10000);
             for (int i =1; i<=20; i++){

                 zab.propose((""+myId+"-proposal "+i).getBytes());
                 Thread.sleep(rnd.nextInt(20)*500l);
             }

             Thread.sleep(10000 + 20*20*500);
             zab.shutdown();


        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            logfile.close();
        }




    }

}
