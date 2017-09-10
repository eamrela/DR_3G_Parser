/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vodafone.dr.configuration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Calendar;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Admin
 */
public class AppConf {

    private static String workingDir;
    private static String mydate = (java.text.DateFormat.getDateInstance().format(Calendar.getInstance().getTime())).replaceAll(":", "");
    
    
    public static boolean configureApp(String path){
        try {
            RandomAccessFile raf = new RandomAccessFile(new File(path), "r");
            String line = null;
            while((line=raf.readLine())!=null){
                if(line.contains("WORKING_DIR")){
                    workingDir = line.split("~")[2];
                }
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(AppConf.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        } catch (IOException ex) {
            Logger.getLogger(AppConf.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
            return true;
    }

    

    public static String getWorkingDir() {
        return workingDir;
    }

    public static void setWorkingDir(String workingDir) {
        AppConf.workingDir = workingDir;
    }

    public static String getMydate() {
        return mydate;
    }

    public static void setMydate(String mydate) {
        AppConf.mydate = mydate;
    }

    

    
     
     
}
