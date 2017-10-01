package socialite.async.util;

import java.io.*;

public class TextUtils {
    public static void writeText(String path, String code) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
            writer.write(code);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String readText(String path) {
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            StringBuilder sb = new StringBuilder();
            String program;
            while ((program = reader.readLine()) != null)
                sb.append(program).append('\n');
            program = sb.toString();
            return program;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
