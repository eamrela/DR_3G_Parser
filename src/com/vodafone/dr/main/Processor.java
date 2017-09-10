/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vodafone.dr.main;

import com.vodafone.dr.configuration.AppConf;
import com.vodafone.dr.mongo.MongoDB;
import com.vodafone.dr.parsers.RNCParser;
import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author Admin
 */
public class Processor {
    
    private static String workingDir = null;
    private static String printoutsDir = null;
    
    public static void initApp(String confPath){
            System.out.println("Initializing App");
            AppConf.configureApp(confPath);
            System.out.println("Initializing Mongo DB");
            MongoDB.initializeDB();
            workingDir = AppConf.getWorkingDir()+"\\DR_3G_"+AppConf.getMydate();
            printoutsDir = workingDir+"\\printouts";
    }
    
    public static void loopAndParse(){
            File [] files = new File(printoutsDir).listFiles();
            ExecutorService executor = Executors.newSingleThreadExecutor();
            System.out.println("Submitting Parsing Tasks");
            for (File file : files) {
                System.out.println("Submitting Task for File: "+file.getName());
                executor.submit(new RNCParser().init(file));
            }
            executor.shutdown();
            while(!executor.isTerminated()){}
            System.out.println("Executer finished: All Files are now parsed and loaded in Db");   
    }
    
    public static void main(String[] args) {
        
        
        if(args.length!=1){
            System.out.println("Please set the input paramters");
            System.out.println("Configuration File");
            System.exit(1);
        }

        String conf = args[0];
        initApp(conf);
        System.out.println("Looping on files now");
        loopAndParse();
        System.out.println("Finished Parsing Files. ");
        
        
    }
}
