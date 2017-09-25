package edu.umich.verdict.jdbc;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class TestBase {
    
    public static String readHost() throws FileNotFoundException {
        ClassLoader classLoader = TestBase.class.getClassLoader();
        File file = new File(classLoader.getResource("integration_test_host.test").getFile());

        Scanner scanner = new Scanner(file);
        String line = scanner.nextLine();
        scanner.close();
        return line;
    }

}
