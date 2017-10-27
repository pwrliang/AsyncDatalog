package socialite.async.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NetworkUtil {
    /***
     * @return return rx[0],tx[1] in bytes
    */
    public static long[] getNetwork() {
        Runtime runtime = Runtime.getRuntime();
        String[] commands = {"ifconfig", "eth0"};
        long rx = 0, tx = 0;
        try {
            Process process = runtime.exec(commands);

            BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            Pattern p = Pattern.compile("RX bytes:(\\d+).*?TX bytes:(\\d+).*?");

            while ((line = stdInput.readLine()) != null) {
                Matcher matcher = p.matcher(line);
                if (matcher.find()) {
                    rx = Long.parseLong(matcher.group(1));
                    tx = Long.parseLong(matcher.group(2));
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new long[]{rx, tx};
    }
}
