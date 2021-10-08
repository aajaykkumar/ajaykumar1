package com.greatlearning.forkjoin;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Collectors;

public class UrlConnectionReader extends RecursiveTask<String> {

    int totalUrl = 0;
    private String[] urls;
    private String url;

    public UrlConnectionReader(String[] urls) {
        this.urls = urls;
        this.totalUrl = urls.length;
    }

    public UrlConnectionReader(String url) {
        this.url = url;
        totalUrl = 1;
    }

    @Override
    protected String compute() {
        System.out.println("Inside compute");
        if (totalUrl > 1) {
            List<UrlConnectionReader> subtasks = new ArrayList<>();
            subtasks.addAll(createSubTasks());
            //Forking the subtask
            for (UrlConnectionReader subtask : subtasks) {
                subtask.fork();
            }
            String result = "";
            for (UrlConnectionReader subtask : subtasks) {
                result += subtask.join();
            }
            return result;

        } else {
            readFromUrlAndWriteTofile(url);
        }
        return url + " process successfully. ";
    }

    private Collection<? extends UrlConnectionReader> createSubTasks() {
        List<UrlConnectionReader> subtasks = new ArrayList<>();
        /*
        Creating subtask for each and every URL.
         */
        for (String url : urls) {
            UrlConnectionReader subtask1 = new UrlConnectionReader(url);
            subtasks.add(subtask1);
        }
        return subtasks;
    }

    private void readFromUrlAndWriteTofile(String urlStr) {
        try {
            URL url = new URL(urlStr);
            try {
                URLConnection urlConnection = url.openConnection();
                InputStream inputStream = urlConnection.getInputStream();
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                String content = bufferedReader.lines().parallel().collect(Collectors.joining("\n"));
                PrintWriter printWriter = new PrintWriter(new File("D:\\output\\output.txt"));
                printWriter.write(content);
                printWriter.flush();
                printWriter.close();
                System.out.println("--");
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }
}
